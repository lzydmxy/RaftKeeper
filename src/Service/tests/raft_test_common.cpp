#include <Service/tests/raft_test_common.h>
#include <Service/NuRaftLogSegment.h>
#include <boost/program_options.hpp>
#include <common/argsToConfig.h>
#include <Poco/File.h>


namespace RK
{

TestServer::TestServer() = default;
TestServer::~TestServer() = default;

void TestServer::init(int argc, char ** argv)
{
    /// Don't parse options with Poco library, we prefer neat boost::program_options
    stopOptionsProcessing();
    /// Save received data into the internal config.
    config().setBool("stacktrace", true);
    config().setBool("logger.console", true);
    config().setString("logger.log", "./test.logs");
    const char * tag = argv[1];
    const char * loglevel = "information";
    if (argc >= 2)
    {
        if (strcmp(tag, "debug"))
        {
            loglevel = "debug";
        }
    }
    config().setString("logger.level", loglevel);

    config().setString("logger.level", "information");

    config().setBool("ignore-error", false);

    std::vector<String> arguments;
    for (int arg_num = 1; arg_num < argc; ++arg_num)
        arguments.emplace_back(argv[arg_num]);
    argsToConfig(arguments, config(), 100);

    if (config().has("logger.console") || config().has("logger.level") || config().has("logger.log"))
    {
        // force enable logging
        config().setString("logger", "logger");
        // sensitive data rules are not used here
        buildLoggers(config(), logger(), "clickhouse-local");
    }
}

void cleanDirectory(const String & log_dir, bool remove_dir)
{
    Poco::File dir_obj(log_dir);
    if (dir_obj.exists())
    {
        std::vector<String> files;
        dir_obj.list(files);
        for (auto & file : files)
        {
            Poco::File(log_dir + "/" + file).remove();
        }
        if (remove_dir)
        {
            dir_obj.remove();
        }
    }
}

void cleanAll()
{
    Poco::File log(LOG_DIR);
    Poco::File snap(LOG_DIR);
    if (log.exists())
        log.remove(true);
    if (snap.exists())
        snap.remove(true);
}

ptr<log_entry> createLogEntry(UInt64 term, const String & key, const String & data)
{
    auto zk_create_request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    zk_create_request->path = key;
    zk_create_request->data = data;
    auto serialized_request = getZooKeeperLogEntry(1, 1, zk_create_request);
    return std::make_shared<log_entry>(term, serialized_request);
}

UInt64 appendEntry(ptr<LogSegmentStore> store, UInt64 term, String & key, String & data)
{
    return store->appendEntry(createLogEntry(term, key, data));
}


ptr<Coordination::ZooKeeperCreateRequest> getRequest(ptr<log_entry> log)
{
    auto zk_create_request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    ReadBufferFromMemory buf(log->get_buf().data_begin(), log->get_buf().size());
    zk_create_request->read(buf);
    return zk_create_request;
}

void setNode(KeeperStore & storage, const String key, const String value, bool is_ephemeral, int64_t session_id)
{
    Coordination::ACLs default_acls;
    Coordination::ACL acl;
    acl.permissions = Coordination::ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    storage.addSessionID(session_id, 30000);

    auto request = cs_new<Coordination::ZooKeeperCreateRequest>();
    request->path = "/" + key;
    request->data = value;
    request->is_ephemeral = is_ephemeral;
    request->is_sequential = false;
    request->acls = default_acls;
    request->xid = 1;
    KeeperStore::KeeperResponsesQueue responses_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    storage.processRequest(responses_queue, {request, session_id, time}, {}, /* check_acl = */ true, /*ignore_response*/ true);
}

}
