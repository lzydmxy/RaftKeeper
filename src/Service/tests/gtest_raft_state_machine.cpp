#include <Service/KeeperStore.h>
#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/KeeperCommon.h>
#include <Service/tests/raft_test_common.h>
#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>
#include <Poco/File.h>
#include <Poco/Logger.h>

using namespace nuraft;
using namespace RK;
using namespace Coordination;

TEST(RaftStateMachine, serializeAndParse)
{
    String snap_dir(SNAP_DIR + "/0");
    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    //NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3);

    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    //UInt64 index = machine.last_commit_index() + 1;
    RequestForSession session_request;
    session_request.session_id = 1;
    auto request = cs_new<ZooKeeperCreateRequest>();
    request->path = "1";
    request->data = "a";
    request->is_ephemeral = false;
    request->is_sequential = false;
    request->acls = default_acls;
    session_request.request = request;

    session_request.create_time = getCurrentTimeMilliseconds();
    session_request.process_time = getCurrentWallTimeMilliseconds();

    ptr<buffer> buf = serializeKeeperRequest(session_request);
    ptr<RequestForSession> session_request_2 = deserializeKeeperRequest(*(buf.get()));
    if (session_request_2->request->getOpNum() == OpNum::Create)
    {
        ZooKeeperCreateRequest * request_2 = static_cast<ZooKeeperCreateRequest *>(session_request_2->request.get());
        ASSERT_EQ(request_2->path, request->path);
        ASSERT_EQ(request_2->data, request->data);
    }

    //machine.shutdown();
    cleanDirectory(snap_dir);
}

TEST(RaftStateMachine, appendEntry)
{
    String snap_dir(SNAP_DIR + "/1");
    String log_dir(LOG_DIR + "/1");

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    String key("/table1");
    String data("CREATE TABLE table1;");
    createZNode(machine, key, data);
    KeeperNode & node = machine.getNode(key);
    ASSERT_EQ(node.data, data);

    machine.shutdown();

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, modifyEntry)
{
    String snap_dir(SNAP_DIR + "/2");
    String log_dir(LOG_DIR + "/2");

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    String key("/table1");
    String data1("CREATE TABLE table1;");
    createZNode(machine, key, data1);
    KeeperNode & node1 = machine.getNode(key);
    ASSERT_EQ(node1.data, data1);

    String data2("CREATE TABLE table2;");
    //op = OP_TYPE_SET;
    setZNode(machine, key, data2);

    KeeperNode & node2 = machine.getNode(key);
    ASSERT_EQ(node2.data, data2);

    removeZNode(machine, key);
    removeZNode(machine, key);
    removeZNode(machine, key);
    KeeperNode & node3 = machine.getNode(key);
    ASSERT_TRUE(node3.data.empty());

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}


TEST(RaftStateMachine, createSnapshot)
{
    auto *log = &(Poco::Logger::get("Test_RaftStateMachine"));
    String snap_dir(SNAP_DIR + "/3");
    String log_dir(LOG_DIR + "/3");

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    LOG_INFO(log, "init last commit index {}", machine.last_commit_index());

    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
    UInt32 last_index = 35;
    for (auto i = 0; i < last_index; i++)
    {
        String key = "/" + std::to_string(i + 1);
        String data = "table_" + key;
        createZNode(machine, key, data);
    }

    sleep(1);

    LOG_INFO(log, "get sm/tm last commit index {},{}", machine.last_commit_index(), machine.getLastCommittedIndex());
    ASSERT_EQ(machine.last_commit_index(), machine.getLastCommittedIndex());

    UInt64 term = 1;
    snapshot meta(last_index, term, config);
    machine.create_snapshot(meta);
    ASSERT_EQ(machine.getStore().getNodesCount(), 38);
    machine.shutdown();

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, syncSnapshot)
{
    String snap_dir_1(SNAP_DIR + "/4");
    String snap_dir_2(SNAP_DIR + "/5");
    cleanDirectory(snap_dir_1);
    cleanDirectory(snap_dir_2);

    String log_dir_1(LOG_DIR + "/4");
    String log_dir_2(LOG_DIR + "/5");
    cleanDirectory(log_dir_1);
    cleanDirectory(log_dir_2);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine_source(
        queue, setting_ptr, snap_dir_1, log_dir_1, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    NuRaftStateMachine machine_target(
        queue, setting_ptr, snap_dir_2, log_dir_2, 10, 3, new_session_id_callback_mutex, new_session_id_callback);

    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
    UInt64 term = 1;
    UInt32 last_index = 1024;
    for (auto i = 0; i < last_index; i++)
    {
        String key = "/" + std::to_string(i + 1);
        String data = "table_" + key;
        createZNode(machine_source, key, data);
    }
    snapshot meta(last_index, term, config);
    machine_source.create_snapshot(meta);

    ptr<buffer> data_out;
    void * user_snp_ctx;
    bool is_last_obj = false;
    ulong obj_id = 0;
    while (!is_last_obj)
    {
        machine_source.read_logical_snp_obj(meta, user_snp_ctx, obj_id, data_out, is_last_obj);
        bool is_first = (obj_id == 0);
        machine_target.save_logical_snp_obj(meta, obj_id, *(data_out.get()), is_first, is_last_obj);
    }
    machine_target.apply_snapshot(meta);
    ASSERT_EQ(machine_target.getStore().getNodesCount(), last_index + 3);

    for (auto i = 1; i < obj_id; i++)
    {
        ASSERT_TRUE(machine_target.existSnapshotObject(meta, i));
    }

    machine_source.shutdown();
    machine_target.shutdown();

    cleanDirectory(snap_dir_1);
    cleanDirectory(snap_dir_2);
    cleanDirectory(log_dir_1);
    cleanDirectory(log_dir_2);
}

TEST(RaftStateMachine, initStateMachine)
{
    auto * log = &(Poco::Logger::get("Test_RaftStateMachine"));
    String snap_dir(SNAP_DIR + "/6");
    String log_dir(LOG_DIR + "/6");

    cleanDirectory(snap_dir, true);
    cleanDirectory(log_dir, true);

    cleanAll();

    //Create
    {
        KeeperResponsesQueue queue;
        RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
        ptr<NuRaftFileLogStore> log_store = cs_new<NuRaftFileLogStore>(log_dir);

        std::mutex new_session_id_callback_mutex;
        std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

        NuRaftStateMachine machine(
            queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback, log_store);

        ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
        UInt32 last_index = 128;
        UInt64 term = 1;

        for (auto i = 0; i < last_index; i++)
        {
            UInt32 index = i + 1;
            String key = "/" + std::to_string(index);
            String data = "table_" + key;
            createZNodeLog(machine, key, data, log_store, term);
        }
        sleep(1);
        LOG_INFO(log, "get sm/tm last commit index {},{}", machine.last_commit_index(), machine.getLastCommittedIndex());
        ASSERT_EQ(machine.last_commit_index(), machine.getLastCommittedIndex());
        snapshot meta(last_index, term, config);
        machine.create_snapshot(meta);

        for (auto i = 0; i < last_index; i++)
        {
            UInt32 index = last_index + i + 1;
            String key = "/" + std::to_string(index);
            String data = "table_" + key;
            createZNodeLog(machine, key, data, log_store, term);
        }
        sleep(1);
        LOG_INFO(log, "get sm/tm last commit index {},{}", machine.last_commit_index(), machine.getLastCommittedIndex());


        ASSERT_EQ(machine.getStore().getNodesCount(), 259);
        machine.shutdown();
    }

    // Load
    {
        KeeperResponsesQueue queue;
        RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
        ptr<NuRaftFileLogStore> log_store = cs_new<NuRaftFileLogStore>(log_dir);

        std::mutex new_session_id_callback_mutex;
        std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

        NuRaftStateMachine machine(
            queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback, log_store);
        LOG_INFO(log, "init last commit index {}", machine.last_commit_index());
        ASSERT_EQ(machine.last_commit_index(), 256);
        machine.shutdown();
    }

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadDoesNotIncreaseZxid)
{
    String snap_dir(SNAP_DIR + "/multiread_zxid");
    String log_dir(LOG_DIR + "/multiread_zxid");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);

    /// Set up: create a session and a test node
    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "test_node", "test_data", /*is_ephemeral=*/false, session_id);

    /// Record zxid before MultiRead
    int64_t zxid_before = machine.getStore().getZxid();

    /// Build MultiRead request
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 100;

    for (int i = 0; i < 5; ++i)
    {
        auto get_req = cs_new<ZooKeeperGetRequest>();
        get_req->path = "/test_node";
        get_req->xid = 100;
        multi_read->requests.push_back(get_req);
    }

    /// Process
    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(
        response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    /// Verify zxid didn't change
    ASSERT_EQ(machine.getStore().getZxid(), zxid_before);

    /// Verify response
    ResponseForSession response_for_session;
    ASSERT_TRUE(response_queue.tryPop(response_for_session));
    ASSERT_EQ(response_for_session.response->getOpNum(), OpNum::MultiRead);
    auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
    ASSERT_EQ(multi_response.responses.size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        ASSERT_EQ(multi_response.responses[i]->error, Error::ZOK);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadRegistersSubrequestWatches)
{
    String snap_dir(SNAP_DIR + "/multiread_watch");
    String log_dir(LOG_DIR + "/multiread_watch");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);

    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "test_node_a", "data_a", /*is_ephemeral=*/false, session_id);
    setNode(machine.getStore(), "test_node_b", "data_b", /*is_ephemeral=*/false, session_id);

    /// Count watches before
    uint64_t watches_before = machine.getStore().getTotalWatchesCount();

    /// Build MultiRead with watched subrequests on different paths
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 200;

    /// Get with watch on path A
    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/test_node_a";
        req->has_watch = true;
        req->xid = 200;
        multi_read->requests.push_back(req);
    }
    /// Exists with watch on path B
    {
        auto req = cs_new<ZooKeeperExistsRequest>();
        req->path = "/test_node_b";
        req->has_watch = true;
        req->xid = 200;
        multi_read->requests.push_back(req);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(
        response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    /// Both subrequests registered watches
    ASSERT_EQ(machine.getStore().getTotalWatchesCount(), watches_before + 2);

    /// Verify response
    ResponseForSession response_for_session;
    ASSERT_TRUE(response_queue.tryPop(response_for_session));
    auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
    ASSERT_EQ(multi_response.responses.size(), 2u);
    ASSERT_EQ(multi_response.responses[0]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[1]->error, Error::ZOK);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadHandlesIndividualErrors)
{
    String snap_dir(SNAP_DIR + "/multiread_error");
    String log_dir(LOG_DIR + "/multiread_error");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);

    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "test_node", "test_data", /*is_ephemeral=*/false, session_id);

    /// Build MultiRead: one valid path, one nonexistent path
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 300;

    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/test_node";
        req->xid = 300;
        multi_read->requests.push_back(req);
    }
    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/nonexistent_path";
        req->xid = 300;
        multi_read->requests.push_back(req);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(
        response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    /// Each subrequest has its own error — first succeeds, second fails with ZNONODE
    ResponseForSession response_for_session;
    ASSERT_TRUE(response_queue.tryPop(response_for_session));
    auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
    ASSERT_EQ(multi_response.responses.size(), 2u);
    ASSERT_EQ(multi_response.responses[0]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[1]->error, Error::ZNONODE);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadRejectsWriteOps)
{
    String snap_dir(SNAP_DIR + "/multiread_reject");
    String log_dir(LOG_DIR + "/multiread_reject");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "test_node", "test_data", false, session_id);

    /// Build MultiRead with a Create subrequest — should be rejected
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 400;

    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/test_node";
        req->xid = 400;
        multi_read->requests.push_back(req);
    }
    {
        Coordination::ACLs acls;
        Coordination::ACL acl;
        acl.permissions = Coordination::ACL::All;
        acl.scheme = "world";
        acl.id = "anyone";
        acls.emplace_back(std::move(acl));
        auto req = cs_new<ZooKeeperCreateRequest>();
        req->path = "/test_node/should_fail";
        req->data = "bad";
        req->acls = acls;
        req->xid = 400;
        multi_read->requests.push_back(req);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    EXPECT_THROW(
        {
            machine.getStore().processRequest(
                response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);
        },
        RK::Exception);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadExistsWatchOnNonExistentNode)
{
    String snap_dir(SNAP_DIR + "/multiread_watch_nonexistent");
    String log_dir(LOG_DIR + "/multiread_watch_nonexistent");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);
    uint64_t watches_before = machine.getStore().getTotalWatchesCount();

    /// Exists with watch on a path that does NOT exist — watch should still be registered
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 500;

    {
        auto req = cs_new<ZooKeeperExistsRequest>();
        req->path = "/nonexistent_path";
        req->has_watch = true;
        req->xid = 500;
        multi_read->requests.push_back(req);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(
        response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    /// Watch must be registered even on ZNONODE for Exists
    ASSERT_EQ(machine.getStore().getTotalWatchesCount(), watches_before + 1);

    /// Response should carry ZNONODE error
    ResponseForSession response_for_session;
    ASSERT_TRUE(response_queue.tryPop(response_for_session));
    auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
    ASSERT_EQ(multi_response.responses.size(), 1u);
    ASSERT_EQ(multi_response.responses[0]->error, Error::ZNONODE);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadAuthCheckPerSubrequest)
{
    String snap_dir(SNAP_DIR + "/multiread_acl");
    String log_dir(LOG_DIR + "/multiread_acl");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    /// Create node A with world-readable ACL (accessible)
    setNode(machine.getStore(), "public_node", "public_data", false, session_id);

    /// Create node B with auth-only ACL (not readable by this session)
    {
        Coordination::ACLs restricted_acls;
        Coordination::ACL acl;
        acl.permissions = Coordination::ACL::All;
        acl.scheme = "digest";
        acl.id = "user:password";
        restricted_acls.emplace_back(std::move(acl));

        auto create_req = cs_new<ZooKeeperCreateRequest>();
        create_req->path = "/restricted_node";
        create_req->data = "secret";
        create_req->is_ephemeral = false;
        create_req->is_sequential = false;
        create_req->acls = restricted_acls;
        create_req->xid = 600;

        KeeperStore::KeeperResponsesQueue rsp_queue;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(
            rsp_queue, {create_req, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/true);
    }

    /// MultiRead with Get on both nodes
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 601;

    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/public_node";
        req->xid = 601;
        multi_read->requests.push_back(req);
    }
    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/restricted_node";
        req->xid = 601;
        multi_read->requests.push_back(req);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(
        response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    ResponseForSession response_for_session;
    ASSERT_TRUE(response_queue.tryPop(response_for_session));
    auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
    ASSERT_EQ(multi_response.responses.size(), 2u);
    /// public_node: readable by world:anyone
    ASSERT_EQ(multi_response.responses[0]->error, Error::ZOK);
    /// restricted_node: auth-only ACL, session has no auth → ZNOAUTH
    ASSERT_EQ(multi_response.responses[1]->error, Error::ZNOAUTH);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}
