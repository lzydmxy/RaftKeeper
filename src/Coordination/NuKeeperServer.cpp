#include <Coordination/NuKeeperServer.h>
#include <Coordination/LoggerWrapper.h>
#include <Coordination/NuKeeperStateMachine.h>
#include <Coordination/InMemoryStateManager.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <chrono>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

NuKeeperServer::NuKeeperServer(
    int server_id_, const std::string & hostname_, int port_,
    const CoordinationSettingsPtr & coordination_settings_,
    ResponsesQueue & responses_queue_)
    : server_id(server_id_)
    , hostname(hostname_)
    , port(port_)
    , endpoint(hostname + ":" + std::to_string(port))
    , coordination_settings(coordination_settings_)
    , state_machine(nuraft::cs_new<NuKeeperStateMachine>(responses_queue_, coordination_settings))
    , state_manager(nuraft::cs_new<InMemoryStateManager>(server_id, endpoint))
    , responses_queue(responses_queue_)
{
}

void NuKeeperServer::addServer(int server_id_, const std::string & server_uri_, bool can_become_leader_, int32_t priority)
{
    nuraft::srv_config config(server_id_, 0, server_uri_, "", /* learner = */ !can_become_leader_, priority);
    auto ret1 = raft_instance->add_srv(config);
    auto code = ret1->get_result_code();
    if (code == nuraft::cmd_result_code::TIMEOUT
        || code == nuraft::cmd_result_code::BAD_REQUEST
        || code == nuraft::cmd_result_code::NOT_LEADER
        || code == nuraft::cmd_result_code::FAILED)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot add server to RAFT quorum with code {}, message '{}'", ret1->get_result_code(), ret1->get_result_str());
}


void NuKeeperServer::startup(bool should_build_quorum)
{
    nuraft::raft_params params;
    params.heart_beat_interval_ = coordination_settings->heart_beat_interval_ms.totalMilliseconds();
    params.election_timeout_lower_bound_ = coordination_settings->election_timeout_lower_bound_ms.totalMilliseconds();
    params.election_timeout_upper_bound_ = coordination_settings->election_timeout_upper_bound_ms.totalMilliseconds();
    params.reserved_log_items_ = coordination_settings->reserved_log_items;
    params.snapshot_distance_ = coordination_settings->snapshot_distance;
    params.client_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds();
    params.auto_forwarding_ = coordination_settings->auto_forwarding;
    params.auto_forwarding_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds() * 2;

    params.return_method_ = nuraft::raft_params::blocking;

    nuraft::asio_service::options asio_opts{};
    nuraft::raft_server::init_options init_options;
    init_options.skip_initial_election_timeout_ = !should_build_quorum;
    init_options.raft_callback_ = [this] (nuraft::cb_func::Type type, nuraft::cb_func::Param * param)
    {
        return callbackFunc(type, param);
    };

    raft_instance = launcher.init(
        state_machine, state_manager, nuraft::cs_new<LoggerWrapper>("RaftInstance", coordination_settings->raft_logs_level), port,
        asio_opts, params, init_options);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot allocate RAFT instance");
}

void NuKeeperServer::shutdown()
{
    state_machine->shutdownStorage();
    if (!launcher.shutdown(coordination_settings->shutdown_timeout.totalSeconds()))
        LOG_WARNING(&Poco::Logger::get("NuKeeperServer"), "Failed to shutdown RAFT server in {} seconds", 5);
}

namespace
{

nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(int64_t session_id, const Coordination::ZooKeeperRequestPtr & request)
{
    DB::WriteBufferFromNuraftBuffer buf;
    DB::writeIntBinary(session_id, buf);
    request->write(buf);
    return buf.getBuffer();
}

}

void NuKeeperServer::putRequest(const NuKeeperStorage::RequestForSession & request_for_session)
{
    auto [session_id, request] = request_for_session;
    if (isLeaderAlive() && request->isReadRequest())
    {
        state_machine->processReadRequest(request_for_session);
    }
    else
    {
        std::vector<nuraft::ptr<nuraft::buffer>> entries;
        entries.push_back(getZooKeeperLogEntry(session_id, request));

        std::lock_guard lock(append_entries_mutex);

        auto result = raft_instance->append_entries(entries);
        if (!result->get_accepted())
        {
            NuKeeperStorage::ResponsesForSessions responses;
            auto response = request->makeResponse();
            response->xid = request->xid;
            response->zxid = 0;
            response->error = Coordination::Error::ZOPERATIONTIMEOUT;
            responses_queue.push(DB::NuKeeperStorage::ResponseForSession{session_id, response});
        }

        if (result->get_result_code() == nuraft::cmd_result_code::TIMEOUT)
        {
            NuKeeperStorage::ResponsesForSessions responses;
            auto response = request->makeResponse();
            response->xid = request->xid;
            response->zxid = 0;
            response->error = Coordination::Error::ZOPERATIONTIMEOUT;
            responses_queue.push(DB::NuKeeperStorage::ResponseForSession{session_id, response});
        }
        else if (result->get_result_code() != nuraft::cmd_result_code::OK)
            throw Exception(ErrorCodes::RAFT_ERROR, "Requests result failed with code {} and message: '{}'", result->get_result_code(), result->get_result_str());
    }
}

int64_t NuKeeperServer::getSessionID(int64_t session_timeout_ms)
{
    auto entry = nuraft::buffer::alloc(sizeof(int64_t));
    /// Just special session request
    nuraft::buffer_serializer bs(entry);
    bs.put_i64(session_timeout_ms);

    std::lock_guard lock(append_entries_mutex);

    auto result = raft_instance->append_entries({entry});

    if (!result->get_accepted())
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot send session_id request to RAFT");

    if (result->get_result_code() != nuraft::cmd_result_code::OK)
        throw Exception(ErrorCodes::RAFT_ERROR, "session_id request failed to RAFT");

    auto resp = result->get();
    if (resp == nullptr)
        throw Exception(ErrorCodes::RAFT_ERROR, "Received nullptr as session_id");

    nuraft::buffer_serializer bs_resp(resp);
    return bs_resp.get_i64();
}

bool NuKeeperServer::isLeader() const
{
    return raft_instance->is_leader();
}

bool NuKeeperServer::isLeaderAlive() const
{
    return raft_instance->is_leader_alive();
}


nuraft::cb_func::ReturnCode NuKeeperServer::callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * /* param */)
{
    if (type == nuraft::cb_func::Type::BecomeFresh || type == nuraft::cb_func::Type::BecomeLeader)
    {
        std::unique_lock lock(initialized_mutex);
        initialized_flag = true;
        initialized_cv.notify_all();
    }
    return nuraft::cb_func::ReturnCode::Ok;
}

bool NuKeeperServer::waitForServer(int32_t id) const
{
    /// FIXME
    for (size_t i = 0; i < 30; ++i)
    {
        if (raft_instance->get_srv_config(id) != nullptr)
            return true;
        LOG_DEBUG(&Poco::Logger::get("NuRaftInit"), "Waiting for server {} to join the cluster", id);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG_DEBUG(&Poco::Logger::get("NuRaftInit"), "Cannot wait for server {}", id);
    return false;
}

void NuKeeperServer::waitInit()
{
    std::unique_lock lock(initialized_mutex);
    int64_t timeout = coordination_settings->startup_timeout.totalMilliseconds();
    if (!initialized_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] { return initialized_flag; }))
        throw Exception(ErrorCodes::RAFT_ERROR, "Failed to wait RAFT initialization");
}

std::unordered_set<int64_t> NuKeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

}
