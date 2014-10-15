#include <DB/Storages/MergeTree/ReplicatedMergeTreeCleanupThread.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>


namespace DB
{

ReplicatedMergeTreeCleanupThread::ReplicatedMergeTreeCleanupThread(StorageReplicatedMergeTree & storage_)
	: storage(storage_),
	log(&Logger::get(storage.database_name + "." + storage.table_name + " (StorageReplicatedMergeTree, CleanupThread)")),
	thread([this] { run(); }) {}


void ReplicatedMergeTreeCleanupThread::run()
{
	const auto CLEANUP_SLEEP_MS = 30 * 1000;

	while (!storage.shutdown_called)
	{
		try
		{
			iterate();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		storage.shutdown_event.tryWait(CLEANUP_SLEEP_MS);
	}

	LOG_DEBUG(log, "Cleanup thread finished");
}


void ReplicatedMergeTreeCleanupThread::iterate()
{
	clearOldParts();

	if (storage.unreplicated_data)
		storage.unreplicated_data->clearOldParts();

	if (storage.is_leader_node)
	{
		clearOldLogs();
		clearOldBlocks();
	}
}


void ReplicatedMergeTreeCleanupThread::clearOldParts()
{
	auto table_lock = storage.lockStructure(false);

	MergeTreeData::DataPartsVector parts = storage.data.grabOldParts();
	size_t count = parts.size();

	if (!count)
	{
		LOG_TRACE(log, "No old parts");
		return;
	}

	try
	{
		while (!parts.empty())
		{
			MergeTreeData::DataPartPtr & part = parts.back();

			LOG_DEBUG(log, "Removing " << part->name);

			zkutil::Ops ops;
			ops.push_back(new zkutil::Op::Remove(storage.replica_path + "/parts/" + part->name + "/columns", -1));
			ops.push_back(new zkutil::Op::Remove(storage.replica_path + "/parts/" + part->name + "/checksums", -1));
			ops.push_back(new zkutil::Op::Remove(storage.replica_path + "/parts/" + part->name, -1));
			auto code = storage.zookeeper->tryMulti(ops);
			if (code != ZOK)
				LOG_WARNING(log, "Couldn't remove " << part->name << " from ZooKeeper: " << zkutil::ZooKeeper::error2string(code));

			part->remove();
			parts.pop_back();
		}
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
		storage.data.addOldParts(parts);
		throw;
	}

	LOG_DEBUG(log, "Removed " << count << " old parts");
}


void ReplicatedMergeTreeCleanupThread::clearOldLogs()
{
	zkutil::Stat stat;
	if (!storage.zookeeper->exists(storage.zookeeper_path + "/log", &stat))
		throw Exception(storage.zookeeper_path + "/log doesn't exist", ErrorCodes::NOT_FOUND_NODE);

	int children_count = stat.numChildren;

	/// Будем ждать, пока накопятся в 1.1 раза больше записей, чем нужно.
	if (static_cast<double>(children_count) < storage.data.settings.replicated_logs_to_keep * 1.1)
		return;

	Strings replicas = storage.zookeeper->getChildren(storage.zookeeper_path + "/replicas", &stat);
	UInt64 min_pointer = std::numeric_limits<UInt64>::max();
	for (const String & replica : replicas)
	{
		String pointer = storage.zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/log_pointer");
		if (pointer.empty())
			return;
		min_pointer = std::min(min_pointer, parse<UInt64>(pointer));
	}

	Strings entries = storage.zookeeper->getChildren(storage.zookeeper_path + "/log");
	std::sort(entries.begin(), entries.end());

	/// Не будем трогать последние replicated_logs_to_keep записей.
	entries.erase(entries.end() - std::min(entries.size(), storage.data.settings.replicated_logs_to_keep), entries.end());
	/// Не будем трогать записи, не меньшие min_pointer.
	entries.erase(std::lower_bound(entries.begin(), entries.end(), "log-" + storage.padIndex(min_pointer)), entries.end());

	if (entries.empty())
		return;

	zkutil::Ops ops;
	for (size_t i = 0; i < entries.size(); ++i)
	{
		ops.push_back(new zkutil::Op::Remove(storage.zookeeper_path + "/log/" + entries[i], -1));

		if (ops.size() > 400 || i + 1 == entries.size())
		{
			/// Одновременно с очисткой лога проверим, не добавилась ли реплика с тех пор, как мы получили список реплик.
			ops.push_back(new zkutil::Op::Check(storage.zookeeper_path + "/replicas", stat.version));
			storage.zookeeper->multi(ops);
			ops.clear();
		}
	}

	LOG_DEBUG(log, "Removed " << entries.size() << " old log entries: " << entries.front() << " - " << entries.back());
}


void ReplicatedMergeTreeCleanupThread::clearOldBlocks()
{
	zkutil::Stat stat;
	if (!storage.zookeeper->exists(storage.zookeeper_path + "/blocks", &stat))
		throw Exception(storage.zookeeper_path + "/blocks doesn't exist", ErrorCodes::NOT_FOUND_NODE);

	int children_count = stat.numChildren;

	/// Чтобы делать "асимптотически" меньше запросов exists, будем ждать, пока накопятся в 1.1 раза больше блоков, чем нужно.
	if (static_cast<double>(children_count) < storage.data.settings.replicated_deduplication_window * 1.1)
		return;

	LOG_TRACE(log, "Clearing about " << static_cast<size_t>(children_count) - storage.data.settings.replicated_deduplication_window
		<< " old blocks from ZooKeeper. This might take several minutes.");

	Strings blocks = storage.zookeeper->getChildren(storage.zookeeper_path + "/blocks");

	std::vector<std::pair<Int64, String> > timed_blocks;

	for (const String & block : blocks)
	{
		zkutil::Stat stat;
		storage.zookeeper->exists(storage.zookeeper_path + "/blocks/" + block, &stat);
		timed_blocks.push_back(std::make_pair(stat.czxid, block));
	}

	zkutil::Ops ops;
	std::sort(timed_blocks.begin(), timed_blocks.end(), std::greater<std::pair<Int64, String>>());
	for (size_t i = storage.data.settings.replicated_deduplication_window; i <  timed_blocks.size(); ++i)
	{
		ops.push_back(new zkutil::Op::Remove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second + "/number", -1));
		ops.push_back(new zkutil::Op::Remove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second + "/columns", -1));
		ops.push_back(new zkutil::Op::Remove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second + "/checksums", -1));
		ops.push_back(new zkutil::Op::Remove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second, -1));

		if (ops.size() > 400 || i + 1 == timed_blocks.size())
		{
			storage.zookeeper->multi(ops);
			ops.clear();
		}
	}

	LOG_TRACE(log, "Cleared " << blocks.size() - storage.data.settings.replicated_deduplication_window << " old blocks from ZooKeeper");
}

}
