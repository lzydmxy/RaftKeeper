#pragma once
#include <DataStreams/BlockIO.h>
#include <Parsers/IAST_fwd.h>

namespace zkutil
{
    class ZooKeeper;
}

namespace DB
{

class Context;
class AccessRightsElements;
struct DDLLogEntry;


/// Pushes distributed DDL query to the queue.
/// Returns DDLQueryStatusInputStream, which reads results of query execution on each host in the cluster.
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, const Context & context);
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, const Context & context, const AccessRightsElements & query_requires_access, bool query_requires_grant_option = false);
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, const Context & context, AccessRightsElements && query_requires_access, bool query_requires_grant_option = false);


class DDLQueryStatusInputStream : public IBlockInputStream
{
public:
    DDLQueryStatusInputStream(const String & zk_node_path, const DDLLogEntry & entry, const Context & context_);

    String getName() const override { return "DDLQueryStatusInputStream"; }

    Block getHeader() const override { return sample; }

    Block getSampleBlock() const { return sample.cloneEmpty(); }

    Block readImpl() override;

private:

    static Strings getChildrenAllowNoNode(const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & node_path);

    Strings getNewAndUpdate(const Strings & current_list_of_finished_hosts);

    String node_path;
    const Context & context;
    Stopwatch watch;
    Poco::Logger * log;

    Block sample;

    NameSet waiting_hosts;  /// hosts from task host list
    NameSet finished_hosts; /// finished hosts from host list
    NameSet ignoring_hosts; /// appeared hosts that are not in hosts list
    Strings current_active_hosts; /// Hosts that were in active state at the last check
    size_t num_hosts_finished = 0;

    /// Save the first detected error and throw it at the end of execution
    std::unique_ptr<Exception> first_exception;

    Int64 timeout_seconds = 120;
};

}
