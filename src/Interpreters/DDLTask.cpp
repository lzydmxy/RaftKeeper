#include <Interpreters/DDLTask.h>
#include <Common/DNSResolver.h>
#include <Common/isLocalAddress.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Poco/Net/NetException.h>
#include <common/logger_useful.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Databases/DatabaseReplicated.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int INCONSISTENT_CLUSTER_DEFINITION;
}

HostID HostID::fromString(const String & host_port_str)
{
    HostID res;
    std::tie(res.host_name, res.port) = Cluster::Address::fromString(host_port_str);
    return res;
}

bool HostID::isLocalAddress(UInt16 clickhouse_port) const
{
    try
    {
        return DB::isLocalAddress(DNSResolver::instance().resolveAddress(host_name, port), clickhouse_port);
    }
    catch (const Poco::Net::NetException &)
    {
        /// Avoid "Host not found" exceptions
        return false;
    }
}


String DDLLogEntry::toString() const
{
    WriteBufferFromOwnString wb;

    Strings host_id_strings(hosts.size());
    std::transform(hosts.begin(), hosts.end(), host_id_strings.begin(), HostID::applyToString);

    auto version = CURRENT_VERSION;
    wb << "version: " << version << "\n";
    wb << "query: " << escape << query << "\n";
    wb << "hosts: " << host_id_strings << "\n";
    wb << "initiator: " << initiator << "\n";

    return wb.str();
}

void DDLLogEntry::parse(const String & data)
{
    ReadBufferFromString rb(data);

    int version;
    rb >> "version: " >> version >> "\n";

    if (version != CURRENT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown DDLLogEntry format version: {}", version);

    Strings host_id_strings;
    rb >> "query: " >> escape >> query >> "\n";
    rb >> "hosts: " >> host_id_strings >> "\n";

    if (!rb.eof())
        rb >> "initiator: " >> initiator >> "\n";
    else
        initiator.clear();

    assertEOF(rb);

    hosts.resize(host_id_strings.size());
    std::transform(host_id_strings.begin(), host_id_strings.end(), hosts.begin(), HostID::fromString);
}


void DDLTaskBase::parseQueryFromEntry(const Context & context)
{
    const char * begin = entry.query.data();
    const char * end = begin + entry.query.size();

    ParserQuery parser_query(end);
    String description;
    query = parseQuery(parser_query, begin, end, description, 0, context.getSettingsRef().max_parser_depth);
}

std::unique_ptr<Context> DDLTaskBase::makeQueryContext(Context & from_context)
{
    auto query_context = std::make_unique<Context>(from_context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(""); // generate random query_id
    query_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    return query_context;
}


bool DDLTask::findCurrentHostID(const Context & global_context, Poco::Logger * log)
{
    bool host_in_hostlist = false;

    for (const HostID & host : entry.hosts)
    {
        auto maybe_secure_port = global_context.getTCPPortSecure();

        /// The port is considered local if it matches TCP or TCP secure port that the server is listening.
        bool is_local_port = (maybe_secure_port && host.isLocalAddress(*maybe_secure_port))
                             || host.isLocalAddress(global_context.getTCPPort());

        if (!is_local_port)
            continue;

        if (host_in_hostlist)
        {
            /// This check could be slow a little bit
            LOG_WARNING(log, "There are two the same ClickHouse instances in task {}: {} and {}. Will use the first one only.",
                             entry_name, host_id.readableString(), host.readableString());
        }
        else
        {
            host_in_hostlist = true;
            host_id = host;
            host_id_str = host.toString();
        }
    }

    return host_in_hostlist;
}

void DDLTask::setClusterInfo(const Context & context, Poco::Logger * log)
{
    auto query_on_cluster = dynamic_cast<ASTQueryWithOnCluster *>(query.get());
    if (!query_on_cluster)
        throw Exception("Received unknown DDL query", ErrorCodes::UNKNOWN_TYPE_OF_QUERY);

    cluster_name = query_on_cluster->cluster;
    cluster = context.tryGetCluster(cluster_name);

    if (!cluster)
        throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
                        "DDL task {} contains current host {} in cluster {}, but there are no such cluster here.",
                        entry_name, host_id.readableString(), cluster_name);

    /// Try to find host from task host list in cluster
    /// At the first, try find exact match (host name and ports should be literally equal)
    /// If the attempt fails, try find it resolving host name of each instance

    if (!tryFindHostInCluster())
    {
        LOG_WARNING(log, "Not found the exact match of host {} from task {} in cluster {} definition. Will try to find it using host name resolving.",
                         host_id.readableString(), entry_name, cluster_name);

        if (!tryFindHostInClusterViaResolving(context))
            throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION, "Not found host {} in definition of cluster {}",
                                                                 host_id.readableString(), cluster_name);

        LOG_INFO(log, "Resolved host {} from task {} as host {} in definition of cluster {}",
                 host_id.readableString(), entry_name, address_in_cluster.readableString(), cluster_name);
    }

    query = query_on_cluster->getRewrittenASTWithoutOnCluster(address_in_cluster.default_database);
    query_on_cluster = nullptr;
}

bool DDLTask::tryFindHostInCluster()
{
    const auto & shards = cluster->getShardsAddresses();
    bool found_exact_match = false;
    String default_database;

    for (size_t shard_num = 0; shard_num < shards.size(); ++shard_num)
    {
        for (size_t replica_num = 0; replica_num < shards[shard_num].size(); ++replica_num)
        {
            const Cluster::Address & address = shards[shard_num][replica_num];

            if (address.host_name == host_id.host_name && address.port == host_id.port)
            {
                if (found_exact_match)
                {
                    if (default_database == address.default_database)
                    {
                        throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
                                        "There are two exactly the same ClickHouse instances {} in cluster {}",
                                        address.readableString(), cluster_name);
                    }
                    else
                    {
                        /* Circular replication is used.
                         * It is when every physical node contains
                         * replicas of different shards of the same table.
                         * To distinguish one replica from another on the same node,
                         * every shard is placed into separate database.
                         * */
                        is_circular_replicated = true;
                        auto * query_with_table = dynamic_cast<ASTQueryWithTableAndOutput *>(query.get());
                        if (!query_with_table || query_with_table->database.empty())
                        {
                            throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
                                            "For a distributed DDL on circular replicated cluster its table name must be qualified by database name.");
                        }
                        if (default_database == query_with_table->database)
                            return true;
                    }
                }
                found_exact_match = true;
                host_shard_num = shard_num;
                host_replica_num = replica_num;
                address_in_cluster = address;
                default_database = address.default_database;
            }
        }
    }

    return found_exact_match;
}

bool DDLTask::tryFindHostInClusterViaResolving(const Context & context)
{
    const auto & shards = cluster->getShardsAddresses();
    bool found_via_resolving = false;

    for (size_t shard_num = 0; shard_num < shards.size(); ++shard_num)
    {
        for (size_t replica_num = 0; replica_num < shards[shard_num].size(); ++replica_num)
        {
            const Cluster::Address & address = shards[shard_num][replica_num];

            if (auto resolved = address.getResolvedAddress();
                resolved && (isLocalAddress(*resolved, context.getTCPPort())
                             || (context.getTCPPortSecure() && isLocalAddress(*resolved, *context.getTCPPortSecure()))))
            {
                if (found_via_resolving)
                {
                    throw Exception(ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
                                    "There are two the same ClickHouse instances in cluster {} : {} and {}",
                                    cluster_name, address_in_cluster.readableString(), address.readableString());
                }
                else
                {
                    found_via_resolving = true;
                    host_shard_num = shard_num;
                    host_replica_num = replica_num;
                    address_in_cluster = address;
                }
            }
        }
    }

    return found_via_resolving;
}

String DDLTask::getShardID() const
{
    /// Generate unique name for shard node, it will be used to execute the query by only single host
    /// Shard node name has format 'replica_name1,replica_name2,...,replica_nameN'
    /// Where replica_name is 'replica_config_host_name:replica_port'

    auto shard_addresses = cluster->getShardsAddresses().at(host_shard_num);

    Strings replica_names;
    for (const Cluster::Address & address : shard_addresses)
        replica_names.emplace_back(address.readableString());
    std::sort(replica_names.begin(), replica_names.end());

    String res;
    for (auto it = replica_names.begin(); it != replica_names.end(); ++it)
        res += *it + (std::next(it) != replica_names.end() ? "," : "");

    return res;
}

DatabaseReplicatedTask::DatabaseReplicatedTask(const String & name, const String & path, DatabaseReplicated * database_)
    : DDLTaskBase(name, path)
    , database(database_)
{
    host_id_str = database->getFullReplicaName();
}

String DatabaseReplicatedTask::getShardID() const
{
    return database->shard_name;
}

std::unique_ptr<Context> DatabaseReplicatedTask::makeQueryContext(Context & from_context)
{
    auto query_context = DDLTaskBase::makeQueryContext(from_context);
    query_context->getClientInfo().query_kind = ClientInfo::QueryKind::REPLICATED_LOG_QUERY; //FIXME why do we need separate query kind?
    query_context->setCurrentDatabase(database->getDatabaseName());

    auto txn = std::make_shared<MetadataTransaction>();
    query_context->initMetadataTransaction(txn);
    txn->current_zookeeper = from_context.getZooKeeper();
    txn->zookeeper_path = database->zookeeper_path;
    txn->is_initial_query = we_are_initiator;

    if (we_are_initiator)
    {
        txn->ops.emplace_back(zkutil::makeRemoveRequest(entry_path + "/try", -1));
        txn->ops.emplace_back(zkutil::makeCreateRequest(entry_path + "/committed", host_id_str, zkutil::CreateMode::Persistent));
        //txn->ops.emplace_back(zkutil::makeRemoveRequest(getActiveNodePath(), -1));
        txn->ops.emplace_back(zkutil::makeSetRequest(database->zookeeper_path + "/max_log_ptr", toString(getLogEntryNumber(entry_name)), -1));
    }

    //if (execute_on_leader)
    //    txn->ops.emplace_back(zkutil::makeCreateRequest(getShardNodePath() + "/executed", host_id_str, zkutil::CreateMode::Persistent));
    //txn->ops.emplace_back(zkutil::makeCreateRequest(getFinishedNodePath(), execution_status.serializeText(), zkutil::CreateMode::Persistent));
    txn->ops.emplace_back(zkutil::makeSetRequest(database->replica_path + "/log_ptr", toString(getLogEntryNumber(entry_name)), -1));

    std::move(ops.begin(), ops.end(), std::back_inserter(txn->ops));
    ops.clear();

    return query_context;
}

String DatabaseReplicatedTask::getLogEntryName(UInt32 log_entry_number)
{
    constexpr size_t seq_node_digits = 10;
    String number = toString(log_entry_number);
    String name = "query-" + String(seq_node_digits - number.size(), '0') + number;
    return name;
}

UInt32 DatabaseReplicatedTask::getLogEntryNumber(const String & log_entry_name)
{
    constexpr const char * name = "query-";
    assert(startsWith(log_entry_name, name));
    return parse<UInt32>(log_entry_name.substr(strlen(name)));
}

void MetadataTransaction::commit()
{
    assert(state == CREATED);
    state = FAILED;
    current_zookeeper->multi(ops);
    state = COMMITED;
}

}
