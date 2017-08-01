#pragma once
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <DataStreams/BlockIO.h>
#include <common/logger_useful.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace DB
{

class ASTAlterQuery;
struct DDLLogEntry;
struct DDLTask;


BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, Context & context);


class DDLWorker
{
public:
    DDLWorker(const std::string & zk_root_dir, Context & context_, const Poco::Util::AbstractConfiguration * config, const String & prefix);
    ~DDLWorker();

    /// Pushes query into DDL queue, returns path to created node
    String enqueueQuery(DDLLogEntry & entry);

    /// Host ID (name:port) for logging purposes
    /// Note that in each task hosts are identified individually by name:port from initiator server cluster config
    std::string getCommonHostID() const
    {
        return host_fqdn_id;
    }

private:
    void processTasks();

    /// Reads entry and check that the host belongs to host list of the task
    /// Returns true and sets current_task if entry parsed and the check is passed
    bool initAndCheckTask(const String & entry_name);


    void processTask(DDLTask & task);

    void processTaskAlter(
        DDLTask & task,
        const ASTAlterQuery * ast_alter,
        const String & rewritten_query,
        const String & node_path);

    void parseQueryAndResolveHost(DDLTask & task);

    bool tryExecuteQuery(const String & query, const DDLTask & task, ExecutionStatus & status);


    /// Checks and cleanups queue's nodes
    void cleanupQueue(const Strings * node_names_to_check = nullptr);


    void createStatusDirs(const std::string & node_name);
    ASTPtr getRewrittenQuery(const DDLLogEntry & node);


    void run();

private:
    Context & context;
    Logger * log = &Logger::get("DDLWorker");

    std::string host_fqdn;      /// current host domain name
    std::string host_fqdn_id;   /// host_name:port

    std::string queue_dir;      /// dir with queue of queries
    std::string master_dir;     /// dir with queries was initiated by the server

    /// Last task that was skipped or sucesfully executed
    std::string last_processed_task_name;

    std::shared_ptr<zkutil::ZooKeeper> zookeeper;

    /// Save state of executed task to avoid duplicate execution on ZK error
    using DDLTaskPtr = std::unique_ptr<DDLTask>;
    DDLTaskPtr current_task;

    std::shared_ptr<Poco::Event> event_queue_updated;
    std::atomic<bool> stop_flag{false};
    std::thread thread;

    size_t last_cleanup_time_seconds = 0;

    /// Delete node if its age is greater than that
    size_t task_max_lifetime = 7 * 24 * 60 * 60; // week (in seconds)
    /// Cleaning starts after new node event is received if the last cleaning wasn't made sooner than N seconds ago
    size_t cleanup_delay_period = 60; // minute (in seconds)

    friend class DDLQueryStatusInputSream;
    friend class DDLTask;
};


}
