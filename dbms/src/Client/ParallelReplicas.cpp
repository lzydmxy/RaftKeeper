#include <DB/Client/ParallelReplicas.h>
#include <boost/concept_check.hpp>

namespace DB
{
	ParallelReplicas::ParallelReplicas(Connection * connection_, const Settings * settings_)
		: settings(settings_),
		active_connection_count(1),
		supports_parallel_execution(false)
	{
		addConnection(connection_);
	}

	ParallelReplicas::ParallelReplicas(std::vector<ConnectionPool::Entry> & entries_, const Settings * settings_) 
		: settings(settings_),
		active_connection_count(entries_.size()),
		supports_parallel_execution(active_connection_count > 1)
	{
		if (supports_parallel_execution && (settings == nullptr))
			throw Exception("Settings are required for parallel execution", ErrorCodes::LOGICAL_ERROR);
		if (active_connection_count == 0)
			throw Exception("No connection specified", ErrorCodes::LOGICAL_ERROR);

		replica_map.reserve(active_connection_count);
		for (auto & entry : entries_)
			addConnection(&*entry);
	}

	void ParallelReplicas::sendExternalTablesData(std::vector<ExternalTablesData> & data)
	{
		if (!sent_query)
			throw Exception("Cannot send external tables data: query not yet sent.", ErrorCodes::LOGICAL_ERROR);

		if (data.size() < active_connection_count)
			throw Exception("Mismatch between replicas and data sources", ErrorCodes::MISMATCH_REPLICAS_DATA_SOURCES);

		auto it = data.begin();
		for (auto & e : replica_map)
		{
			Connection * connection = e.second;
			if (connection != nullptr)
				connection->sendExternalTablesData(*it);
			++it;
		}
	}

	void ParallelReplicas::sendQuery(const String & query, const String & query_id, UInt64 stage, bool with_pending_data)
	{
		if (sent_query)
			throw Exception("Query already sent.", ErrorCodes::LOGICAL_ERROR);

		if (supports_parallel_execution)
		{
			Settings query_settings = *settings;
			query_settings.parallel_replicas_count = active_connection_count;
			UInt64 offset = 0;

			for (auto & e : replica_map)
			{
				Connection * connection = e.second;
				if (connection != nullptr)
				{
					query_settings.parallel_replica_offset = offset;
					connection->sendQuery(query, query_id, stage, &query_settings, with_pending_data);
					++offset;
				}
			}

			if (offset > 0)
				sent_query = true;
		}
		else
		{
			auto it = replica_map.begin();
			Connection * connection = it->second;
			if (connection != nullptr)
			{
				connection->sendQuery(query, query_id, stage, settings, with_pending_data);
				sent_query = true;
			}
		}
	}

	Connection::Packet ParallelReplicas::receivePacket()
	{
		if (!sent_query)
			throw Exception("Cannot receive packets: no query sent.", ErrorCodes::LOGICAL_ERROR);
		if (!hasActiveConnections())
			throw Exception("No more packets are available.", ErrorCodes::LOGICAL_ERROR);

		auto it = getConnection();
		if (it == replica_map.end())
			throw Exception("No available replica", ErrorCodes::NO_AVAILABLE_REPLICA);

		Connection * & connection = it->second;
		Connection::Packet packet = connection->receivePacket();

		switch (packet.type)
		{
			case Protocol::Server::Data:
			case Protocol::Server::Progress:
			case Protocol::Server::ProfileInfo:
			case Protocol::Server::Totals:
			case Protocol::Server::Extremes:
				break;

			case Protocol::Server::EndOfStream:
			case Protocol::Server::Exception:
			default:
				invalidateConnection(connection);
				break;
		}

		return packet;
	}

	void ParallelReplicas::disconnect()
	{
		for (auto & e : replica_map)
		{
			Connection * & connection = e.second;
			if (connection != nullptr)
			{
				connection->disconnect();
				invalidateConnection(connection);
			}
		}
	}

	void ParallelReplicas::sendCancel()
	{
		if (!sent_query || cancelled)
			throw Exception("Cannot cancel. Either no query sent or already cancelled.", ErrorCodes::LOGICAL_ERROR);

		for (auto & e : replica_map)
		{
			Connection * connection = e.second;
			if (connection != nullptr)
				connection->sendCancel();
		}

		cancelled = true;
	}

	Connection::Packet ParallelReplicas::drain()
	{
		if (!cancelled)
			throw Exception("Cannot drain connections: cancel first.", ErrorCodes::LOGICAL_ERROR);

		Connection::Packet res;
		res.type = Protocol::Server::EndOfStream;

		while (hasActiveConnections())
		{
			Connection::Packet packet = receivePacket();

			switch (packet.type)
			{
				case Protocol::Server::Data:
				case Protocol::Server::Progress:
				case Protocol::Server::ProfileInfo:
				case Protocol::Server::Totals:
				case Protocol::Server::Extremes:
				case Protocol::Server::EndOfStream:
					break;

				case Protocol::Server::Exception:
				default:
					res = packet;
					break;
			}
		}

		return res;
	}

	std::string ParallelReplicas::dumpAddresses() const
	{
		bool is_first = true;
		std::ostringstream os;
		for (auto & e : replica_map)
		{
			const Connection * connection = e.second;
			if (connection != nullptr)
			{
				os << (is_first ? "" : "; ") << connection->getServerAddress();
				if (is_first) { is_first = false; }
			}
		}

		return os.str();
	}

	void ParallelReplicas::addConnection(Connection * connection)
	{
		if (connection == nullptr)
			throw Exception("Invalid connection specified in parameter.", ErrorCodes::LOGICAL_ERROR);
		auto res = replica_map.insert(std::make_pair(connection->socket.impl()->sockfd(), connection));
		if (!res.second)
			throw Exception("Invalid set of connections.", ErrorCodes::LOGICAL_ERROR);
	}

	void ParallelReplicas::invalidateConnection(Connection * & connection)
	{
		connection = nullptr;
		--active_connection_count;
	}

	ParallelReplicas::ReplicaMap::iterator ParallelReplicas::getConnection()
	{
		ReplicaMap::iterator it;

		if (supports_parallel_execution)
			it = waitForReadEvent();
		else
		{
			it = replica_map.begin();
			if (it->second == nullptr)
				it = replica_map.end();
		}

		return it;
	}

	ParallelReplicas::ReplicaMap::iterator ParallelReplicas::waitForReadEvent()
	{
		Poco::Net::Socket::SocketList read_list;
		read_list.reserve(active_connection_count);

		for (auto & e : replica_map)
		{
			Connection * connection = e.second;
			if ((connection != nullptr) && connection->hasReadBufferPendingData())
				read_list.push_back(connection->socket);
		}

		if (read_list.empty())
		{
			Poco::Net::Socket::SocketList write_list;
			Poco::Net::Socket::SocketList except_list;

			for (auto & e : replica_map)
			{
				Connection * connection = e.second;
				if (connection != nullptr)
					read_list.push_back(connection->socket);
			}
			int n = Poco::Net::Socket::select(read_list, write_list, except_list, settings->poll_interval * 1000000);
			if (n == 0)
				return replica_map.end();
		}

		auto & socket = read_list[rand() % read_list.size()];
		return replica_map.find(socket.impl()->sockfd());
	}
}
