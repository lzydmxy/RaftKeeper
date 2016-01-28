#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>
#include <DB/DataStreams/BlockExtraInfoInputStream.h>
#include <DB/DataStreams/UnionBlockInputStream.h>

#include <DB/Storages/StorageDistributed.h>
#include <DB/Storages/VirtualColumnFactory.h>
#include <DB/Storages/Distributed/DistributedBlockOutputStream.h>
#include <DB/Storages/Distributed/DirectoryMonitor.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>
#include <DB/Parsers/ParserAlterQuery.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ASTWeightedZooKeeperPath.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Interpreters/InterpreterDescribeQuery.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Interpreters/ClusterProxy/Query.h>
#include <DB/Interpreters/ClusterProxy/SelectQueryConstructor.h>
#include <DB/Interpreters/ClusterProxy/DescribeQueryConstructor.h>
#include <DB/Interpreters/ClusterProxy/AlterQueryConstructor.h>

#include <DB/Core/Field.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
	extern const int STORAGE_REQUIRES_PARAMETER;
}


namespace
{
	/// select query has database and table names as AST pointers
	/// Создает копию запроса, меняет имена базы данных и таблицы.
	inline ASTPtr rewriteSelectQuery(const ASTPtr & query, const std::string & database, const std::string & table)
	{
		auto modified_query_ast = query->clone();

		auto & actual_query = typeid_cast<ASTSelectQuery &>(*modified_query_ast);
		actual_query.database = new ASTIdentifier{{}, database, ASTIdentifier::Database};
		actual_query.table = new ASTIdentifier{{}, table, ASTIdentifier::Table};

		return modified_query_ast;
	}

	/// insert query has database and table names as bare strings
	/// Создает копию запроса, меняет имена базы данных и таблицы.
	inline ASTPtr rewriteInsertQuery(const ASTPtr & query, const std::string & database, const std::string & table)
	{
		auto modified_query_ast = query->clone();

		auto & actual_query = typeid_cast<ASTInsertQuery &>(*modified_query_ast);
		actual_query.database = database;
		actual_query.table = table;
		/// make sure query is not INSERT SELECT
		actual_query.select = nullptr;

		return modified_query_ast;
	}
}


StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	Cluster & cluster_,
	Context & context_,
	const ASTPtr & sharding_key_,
	const String & data_path_)
	: name(name_), columns(columns_),
	remote_database(remote_database_), remote_table(remote_table_),
	context(context_), cluster(cluster_),
	sharding_key_expr(sharding_key_ ? ExpressionAnalyzer(sharding_key_, context, nullptr, *columns).getActions(false) : nullptr),
	sharding_key_column_name(sharding_key_ ? sharding_key_->getColumnName() : String{}),
	write_enabled(!data_path_.empty() && (((cluster.getLocalShardCount() + cluster.getRemoteShardCount()) < 2) || sharding_key_)),
	path(data_path_.empty() ? "" : (data_path_ + escapeForFileName(name) + '/'))
{
	createDirectoryMonitors();
}

StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	const String & remote_database_,
	const String & remote_table_,
	Cluster & cluster_,
	Context & context_,
	const ASTPtr & sharding_key_,
	const String & data_path_)
	: IStorage{materialized_columns_, alias_columns_, column_defaults_},
	name(name_), columns(columns_),
	remote_database(remote_database_), remote_table(remote_table_),
	context(context_), cluster(cluster_),
	sharding_key_expr(sharding_key_ ? ExpressionAnalyzer(sharding_key_, context, nullptr, *columns).getActions(false) : nullptr),
	sharding_key_column_name(sharding_key_ ? sharding_key_->getColumnName() : String{}),
	write_enabled(!data_path_.empty() && (((cluster.getLocalShardCount() + cluster.getRemoteShardCount()) < 2) || sharding_key_)),
	path(data_path_.empty() ? "" : (data_path_ + escapeForFileName(name) + '/'))
{
	createDirectoryMonitors();
}

StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	const String & remote_database_,
	const String & remote_table_,
	const String & cluster_name,
	Context & context_,
	const ASTPtr & sharding_key_,
	const String & data_path_)
{
	context_.initClusters();

	return (new StorageDistributed{
		name_, columns_,
		materialized_columns_, alias_columns_, column_defaults_,
		remote_database_, remote_table_,
		context_.getCluster(cluster_name), context_,
		sharding_key_, data_path_
	})->thisPtr();
}


StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	SharedPtr<Cluster> & owned_cluster_,
	Context & context_)
{
	auto res = new StorageDistributed{
		name_, columns_, remote_database_,
		remote_table_, *owned_cluster_, context_
	};

	/// Захватываем владение объектом-кластером.
	res->owned_cluster = owned_cluster_;

	return res->thisPtr();
}

BlockInputStreams StorageDistributed::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	size_t result_size = (cluster.getRemoteShardCount() * settings.max_parallel_replicas) + cluster.getLocalShardCount();

	processed_stage = result_size == 1 || settings.distributed_group_by_no_merge
		? QueryProcessingStage::Complete
		: QueryProcessingStage::WithMergeableState;

	const auto & modified_query_ast = rewriteSelectQuery(
		query, remote_database, remote_table);

	Tables external_tables;

	if (settings.global_subqueries_method == GlobalSubqueriesMethod::PUSH)
		external_tables = context.getExternalTables();

	/// Отключаем мультиплексирование шардов, если есть ORDER BY без GROUP BY.
	//const ASTSelectQuery & ast = *(static_cast<const ASTSelectQuery *>(modified_query_ast.get()));

	/** Функциональность shard_multiplexing не доделана - выключаем её.
	  * (Потому что установка соединений с разными шардами в рамках одного потока выполняется не параллельно.)
	  * Подробнее смотрите в https://███████████.yandex-team.ru/METR-18300
	  */
	//bool enable_shard_multiplexing = !(ast.order_expression_list && !ast.group_expression_list);
	bool enable_shard_multiplexing = false;

	ClusterProxy::SelectQueryConstructor select_query_constructor(processed_stage, external_tables);

	return ClusterProxy::Query(select_query_constructor, cluster, modified_query_ast,
		context, settings, enable_shard_multiplexing).execute();
}

BlockOutputStreamPtr StorageDistributed::write(ASTPtr query, const Settings & settings)
{
	if (!write_enabled)
		throw Exception{
			"Method write is not supported by storage " + getName() +
			" with more than one shard and no sharding key provided",
			ErrorCodes::STORAGE_REQUIRES_PARAMETER
		};

	return new DistributedBlockOutputStream{
		*this,
		rewriteInsertQuery(query, remote_database, remote_table)
	};
}

void StorageDistributed::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
{
	auto lock = lockStructureForAlter();
	params.apply(*columns, materialized_columns, alias_columns, column_defaults);
	InterpreterAlterQuery::updateMetadata(database_name, table_name,
		*columns, materialized_columns, alias_columns, column_defaults, context);
}

void StorageDistributed::shutdown()
{
	directory_monitors.clear();
}

void StorageDistributed::reshardPartitions(const String & database_name, const Field & first_partition,
	const Field & last_partition, const WeightedZooKeeperPaths & weighted_zookeeper_paths,
	const ASTPtr & sharding_key_expr, const Settings & settings)
{
	/// Создать запрос ALTER TABLE xxx.yyy RESHARD PARTITION zzz TO ttt USING uuu.

	ASTPtr alter_query_ptr = new ASTAlterQuery;
	auto & alter_query = static_cast<ASTAlterQuery &>(*alter_query_ptr);

	alter_query.database = remote_database;
	alter_query.table = remote_table;

	alter_query.parameters.emplace_back();
	ASTAlterQuery::Parameters & parameters = alter_query.parameters.back();

	parameters.type = ASTAlterQuery::RESHARD_PARTITION;
	if (!first_partition.isNull())
		parameters.partition = new ASTLiteral({}, first_partition);
	if (!last_partition.isNull())
		parameters.last_partition = new ASTLiteral({}, last_partition);

	ASTPtr expr_list = new ASTExpressionList;
	for (const auto & entry : weighted_zookeeper_paths)
	{
		ASTPtr weighted_path_ptr = new ASTWeightedZooKeeperPath;
		auto & weighted_path = static_cast<ASTWeightedZooKeeperPath &>(*weighted_path_ptr);
		weighted_path.path = entry.first;
		weighted_path.weight = entry.second;
		expr_list->children.push_back(weighted_path_ptr);
	}

	parameters.weighted_zookeeper_paths = expr_list;
	parameters.sharding_key_expr = sharding_key_expr;

	/** Функциональность shard_multiplexing не доделана - выключаем её.
	  * (Потому что установка соединений с разными шардами в рамках одного потока выполняется не параллельно.)
	  * Подробнее смотрите в https://███████████.yandex-team.ru/METR-18300
	  */
	bool enable_shard_multiplexing = false;

	ClusterProxy::AlterQueryConstructor alter_query_constructor;

	BlockInputStreams streams = ClusterProxy::Query(alter_query_constructor, cluster, alter_query_ptr,
		context, settings, enable_shard_multiplexing).execute();

	streams[0] = new UnionBlockInputStream<>(streams, nullptr, settings.max_distributed_connections);
	streams.resize(1);

	auto stream_ptr = dynamic_cast<IProfilingBlockInputStream *>(&*streams[0]);
	if (stream_ptr == nullptr)
		throw Exception("StorageDistributed: Internal error", ErrorCodes::LOGICAL_ERROR);
	auto & stream = *stream_ptr;

	while (!stream.isCancelled() && stream.read())
		;
}

BlockInputStreams StorageDistributed::describe(const Context & context, const Settings & settings)
{
	/// Создать запрос DESCRIBE TABLE.

	ASTPtr describe_query_ptr = new ASTDescribeQuery;
	auto & describe_query = static_cast<ASTDescribeQuery &>(*describe_query_ptr);

	describe_query.database = remote_database;
	describe_query.table = remote_table;

	/** Функциональность shard_multiplexing не доделана - выключаем её.
	  * (Потому что установка соединений с разными шардами в рамках одного потока выполняется не параллельно.)
	  * Подробнее смотрите в https://███████████.yandex-team.ru/METR-18300
	  */
	bool enable_shard_multiplexing = false;

	ClusterProxy::DescribeQueryConstructor describe_query_constructor;

	return ClusterProxy::Query(describe_query_constructor, cluster, describe_query_ptr,
		context, settings, enable_shard_multiplexing).execute();
}

NameAndTypePair StorageDistributed::getColumn(const String & column_name) const
{
	if (const auto & type = VirtualColumnFactory::tryGetType(column_name))
		return { column_name, type };

	return getRealColumn(column_name);
}

bool StorageDistributed::hasColumn(const String & column_name) const
{
	return VirtualColumnFactory::hasColumn(column_name) || IStorage::hasColumn(column_name);
}

void StorageDistributed::createDirectoryMonitor(const std::string & name)
{
	directory_monitors.emplace(name, std::make_unique<DirectoryMonitor>(*this, name));
}

void StorageDistributed::createDirectoryMonitors()
{
	if (path.empty())
		return;

	Poco::File{path}.createDirectory();

	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it{path}; it != end; ++it)
		if (it->isDirectory())
			createDirectoryMonitor(it.name());
}

void StorageDistributed::requireDirectoryMonitor(const std::string & name)
{
	if (!directory_monitors.count(name))
		createDirectoryMonitor(name);
}

size_t StorageDistributed::getShardCount() const
{
	return cluster.getRemoteShardCount();
}

}
