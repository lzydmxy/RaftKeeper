#include <chrono>
#include <string>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Service/KeeperServer.h>
#include <Service/LoggerWrapper.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/NuRaftStateManager.h>
#include <Service/ReadBufferFromNuraftBuffer.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <boost/algorithm/string.hpp>
#include <libnuraft/async.hxx>
#include <Poco/NumberFormatter.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>

namespace RK
{
namespace ErrorCodes
{
    extern const int RAFT_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
}

using Poco::NumberFormatter;

namespace
{
    std::string checkAndGetSuperdigest(const String & user_and_digest)
    {
        if (user_and_digest.empty())
            return "";

        std::vector<std::string> scheme_and_id;
        boost::split(scheme_and_id, user_and_digest, [](char c) { return c == ':'; });
        if (scheme_and_id.size() != 2 || scheme_and_id[0] != "super")
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER, "Incorrect superdigest in keeper_server config. Must be 'super:base64string'");

        return user_and_digest;
    }

}
KeeperServer::KeeperServer(
    const SettingsPtr & settings_,
    const Poco::Util::AbstractConfiguration & config_,
    SvsKeeperResponsesQueue & responses_queue_,
    std::shared_ptr<RequestProcessor> request_processor_)
    : server_id(settings_->my_id)
    , settings(settings_)
    , config(config_)
    , responses_queue(responses_queue_)
    , log(&(Poco::Logger::get("KeeperServer")))
{
    state_manager = cs_new<NuRaftStateManager>(server_id, config, settings_);

    state_machine = nuraft::cs_new<NuRaftStateMachine>(
        responses_queue_,
        settings->raft_settings,
        settings->snapshot_dir,
        settings->snapshot_start_time,
        settings->snapshot_end_time,
        settings->snapshot_create_interval,
        settings->raft_settings->max_stored_snapshots,
        new_session_id_callback_mutex,
        new_session_id_callback,
        state_manager->load_log_store(),
        checkAndGetSuperdigest(settings->super_digest),
        KeeperSnapshotStore::MAX_OBJECT_NODE_SIZE,
        request_processor_);
}


void KeeperServer::startup()
{
    auto raft_settings = settings->raft_settings;
    nuraft::raft_params params;
    params.heart_beat_interval_ = raft_settings->heart_beat_interval_ms;
    params.election_timeout_lower_bound_ = raft_settings->election_timeout_lower_bound_ms;
    params.election_timeout_upper_bound_ = raft_settings->election_timeout_upper_bound_ms;
    params.reserved_log_items_ = raft_settings->reserved_log_items;
    params.snapshot_distance_ = raft_settings->snapshot_distance;
    params.client_req_timeout_ = raft_settings->operation_timeout_ms;
    params.return_method_ = nuraft::raft_params::blocking;
    params.parallel_log_appending_ = raft_settings->log_fsync_mode == FsyncMode::FSYNC_PARALLEL;
    params.auto_forwarding_ = true;

    nuraft::asio_service::options asio_opts{};
    asio_opts.thread_pool_size_ = raft_settings->nuraft_thread_size;
    nuraft::raft_server::init_options init_options;
    init_options.skip_initial_election_timeout_ = state_manager->shouldStartAsFollower();
    init_options.raft_callback_ = [this](nuraft::cb_func::Type type, nuraft::cb_func::Param * param) { return callbackFunc(type, param); };

    UInt16 port = config.getInt("keeper.internal_port", 8103);

    raft_instance = launcher.init(
        state_machine,
        state_manager,
        nuraft::cs_new<LoggerWrapper>("NuRaft", raft_settings->raft_logs_level),
        port,
        asio_opts,
        params,
        init_options);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot allocate RAFT instance");

    /// used raft_instance notify_log_append_completion
    if (raft_settings->log_fsync_mode == FsyncMode::FSYNC_PARALLEL)
        dynamic_cast<NuRaftFileLogStore &>(*state_manager->load_log_store()).setRaftServer(raft_instance);
}

void KeeperServer::addServer(const std::vector<std::string> & tokens)
{
    if (tokens.size() < 2)
    {
        LOG_ERROR(log, "Too few arguments.");
        return;
    }

    int server_id_to_add = atoi(tokens[0].c_str());
    if (!server_id_to_add || server_id_to_add == server_id)
    {
        LOG_WARNING(log, "Wrong server id: {}", server_id_to_add);
        return;
    }

    std::string endpoint_to_add = tokens[1];
    srv_config srv_conf_to_add(server_id_to_add, 1, endpoint_to_add, std::string(), false, 50);
    LOG_DEBUG(log, "Adding server {}, {}.", toString(server_id_to_add), endpoint_to_add);
    auto ret = raft_instance->add_srv(srv_conf_to_add);
    if (!ret->get_accepted() || ret->get_result_code() != nuraft::cmd_result_code::OK)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(30000));
        auto ret1 = raft_instance->add_srv(srv_conf_to_add);
        if (!ret1->get_accepted() || ret1->get_result_code() != nuraft::cmd_result_code::OK)
        {
            LOG_ERROR(log, "Failed to add server: {}", ret1->get_result_code());
            return;
        }
    }

    // Wait until it appears in server list.
    const size_t MAX_TRY = 400;
    for (size_t jj = 0; jj < MAX_TRY; ++jj)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        ptr<srv_config> conf = raft_instance->get_srv_config(server_id_to_add);
        if (conf)
        {
            LOG_DEBUG(log, "Add server done.");
            break;
        }
    }
    LOG_DEBUG(log, "Async request is in progress add server {}.", server_id_to_add);
}

void KeeperServer::addServer(ptr<srv_config> srv_conf_to_add)
{
    LOG_DEBUG(log, "Adding server {}, {} after 10s.", toString(srv_conf_to_add->get_id()), srv_conf_to_add->get_endpoint());
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    auto ret = raft_instance->add_srv(*srv_conf_to_add);
    if (!ret->get_accepted() || ret->get_result_code() != nuraft::cmd_result_code::OK)
    {
        LOG_DEBUG(log, "Retry adding server {}, {} after 30s.", toString(srv_conf_to_add->get_id()), srv_conf_to_add->get_endpoint());
        std::this_thread::sleep_for(std::chrono::milliseconds(30000));
        auto ret1 = raft_instance->add_srv(*srv_conf_to_add);
        if (!ret1->get_accepted() || ret1->get_result_code() != nuraft::cmd_result_code::OK)
        {
            LOG_ERROR(log, "Failed to add server {} : {}", srv_conf_to_add->get_endpoint(), ret1->get_result_code());
            return;
        }
    }

    // Wait until it appears in server list.
    const size_t MAX_TRY = 4 * 60 * 10; // 10 minutes
    for (size_t jj = 0; jj < MAX_TRY; ++jj)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        ptr<srv_config> conf = raft_instance->get_srv_config(srv_conf_to_add->get_id());
        if (conf)
        {
            LOG_DEBUG(log, "Add server {} done.", srv_conf_to_add->get_endpoint());
            return;
        }
    }
    LOG_DEBUG(log, "Async request is in progress add server {}.", srv_conf_to_add->get_endpoint());
}


void KeeperServer::getServerList(std::vector<ServerInfo> & server_list)
{
    std::vector<ptr<srv_config>> configs;
    raft_instance->get_srv_config_all(configs);
    int32 leader_id = raft_instance->get_leader();

    for (auto & entry : configs)
    {
        ptr<srv_config> & srv = entry;
        //std::cout << "server id " << srv->get_id() << ": " << srv->get_endpoint();
        ServerInfo server;
        server.server_id = srv->get_id();
        server.endpoint = srv->get_endpoint();
        if (srv->get_id() == leader_id)
        {
            //std::cout << " (LEADER)";
            server.is_leader = true;
        }
        else
        {
            server.is_leader = false;
        }
        server_list.push_back(server);
        //std::cout << std::endl;
    }
}

ptr<ForwardingConnection> KeeperServer::getLeaderClient(size_t thread_idx)
{
    return state_manager->getClient(raft_instance->get_leader(), thread_idx);
}


int32 KeeperServer::getLeader()
{
    return raft_instance->get_leader();
}

void KeeperServer::removeServer(const std::string & endpoint)
{
    std::vector<ServerInfo> server_list;
    getServerList(server_list);
    std::optional<UInt32> to_remove_id;
    for (auto it = server_list.begin(); it != server_list.end(); it++)
    {
        if (it->endpoint == endpoint)
        {
            to_remove_id = it->server_id;
            LOG_DEBUG(log, "Removing server {}, {} after 30s.", toString(it->server_id), endpoint);
            std::this_thread::sleep_for(std::chrono::milliseconds(30000));
            auto ret = raft_instance->remove_srv(it->server_id);
            if (!ret->get_accepted() || ret->get_result_code() != nuraft::cmd_result_code::OK)
            {
                LOG_DEBUG(log, "Retry removing server {}, {} after 30s.", toString(it->server_id), endpoint);
                std::this_thread::sleep_for(std::chrono::milliseconds(30000));
                auto ret1 = raft_instance->remove_srv(it->server_id);
                if (!ret1->get_accepted() || ret1->get_result_code() != nuraft::cmd_result_code::OK)
                {
                    LOG_ERROR(log, "Failed to remove server {} : {}", endpoint, ret1->get_result_code());
                    return;
                }
            }
            break;
        }
    }

    if (!to_remove_id)
        return;
    // Wait until it appears in server list.
    const size_t MAX_TRY = 400;
    for (size_t jj = 0; jj < MAX_TRY; ++jj)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        ptr<srv_config> conf = raft_instance->get_srv_config(to_remove_id.value());
        if (!conf)
        {
            LOG_DEBUG(log, "Remove server {} done.", endpoint);
            return;
        }
    }
    LOG_DEBUG(log, "Async request is in progress remove server {}.", endpoint);
}


void KeeperServer::shutdown()
{
    LOG_INFO(log, "Shutting down keeper server.");
    state_machine->shutdown();
    if (state_manager->load_log_store() && !state_manager->load_log_store()->flush())
        LOG_WARNING(log, "Log store flush error while server shutdown.");
    //    state_manager->flushLogStore();

    dynamic_cast<NuRaftFileLogStore &>(*state_manager->load_log_store()).shutdown();

    if (!launcher.shutdown(settings->raft_settings->shutdown_timeout))
        LOG_WARNING(log, "Failed to shutdown RAFT server in {} seconds", 5);
    LOG_INFO(log, "Shut down keeper server done!");
}

namespace
{
    nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(int64_t session_id, int64_t time, const Coordination::ZooKeeperRequestPtr & request)
    {
        RK::WriteBufferFromNuraftBuffer buf;
        RK::writeIntBinary(session_id, buf);
        request->write(buf);
        Coordination::write(time, buf);
        return buf.getBuffer();
    }
}

void KeeperServer::putRequest(const KeeperStore::RequestForSession & request_for_session)
{
    auto [session_id, request, time, server, client] = request_for_session;
    if (isLeaderAlive() && request->isReadRequest())
    {
        LOG_TRACE(
            log, "[put read request]SessionID/xid #{}#{}, opnum {}", session_id, request->xid, Coordination::toString(request->getOpNum()));
        state_machine->processReadRequest(request_for_session);
    }
    else
    {
        std::vector<ptr<buffer>> entries;
        entries.push_back(getZooKeeperLogEntry(session_id, time, request));

        LOG_TRACE(
            log,
            "[put write request]SessionID/xid #{}#{}, opnum {}, entries {}",
            session_id,
            request->xid,
            Coordination::toString(request->getOpNum()),
            entries.size());

        ptr<nuraft::cmd_result<ptr<buffer>>> result;
        {
            //            std::lock_guard lock(append_entries_mutex);
            result = raft_instance->append_entries(entries);
        }

        if (!result->has_result())
            result->get();

        if (result->get_accepted() && result->get_result_code() == nuraft::cmd_result_code::OK)
        {
            /// response pushed into queue by state machine
            return;
        }

        auto response = request->makeResponse();

        response->xid = request->xid;
        response->zxid = 0;

        response->error = result->get_result_code() == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
                                                                                        : Coordination::Error::ZCONNECTIONLOSS;

        responses_queue.push(RK::KeeperStore::ResponseForSession{session_id, response});
        if (!result->get_accepted())
            throw Exception(ErrorCodes::RAFT_ERROR, "Request session {} xid {} error, result is not accepted.", session_id, request->xid);
        else
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Request session {} xid {} error, nuraft code {} and message: '{}'",
                session_id,
                request->xid,
                result->get_result_code(),
                result->get_result_str());
    }
}

ptr<nuraft::cmd_result<ptr<buffer>>> KeeperServer::putRequestBatch(const std::vector<KeeperStore::RequestForSession> & request_batch)
{
    LOG_DEBUG(log, "process the batch requests {}", request_batch.size());
    std::vector<ptr<buffer>> entries;
    for (const auto & request_session : request_batch)
    {
        LOG_TRACE(
            log,
            "push request to entries session {}, xid {}, opnum {}",
            request_session.session_id,
            request_session.request->xid,
            request_session.request->getOpNum());
        entries.push_back(getZooKeeperLogEntry(request_session.session_id, request_session.create_time, request_session.request));
    }
    /// append_entries write request
    ptr<nuraft::cmd_result<ptr<buffer>>> result = raft_instance->append_entries(entries);
    return result;
}

void KeeperServer::processReadRequest(const KeeperStore::RequestForSession & request_for_session)
{
    auto [session_id, request, time, server, client] = request_for_session;
    if (isLeaderAlive() && request->isReadRequest())
    {
        state_machine->processReadRequest(request_for_session);
    }
}

int64_t KeeperServer::getSessionID(int64_t session_timeout_ms)
{
    auto entry = buffer::alloc(sizeof(int64_t));
    /// Just special session request
    nuraft::buffer_serializer bs(entry);
    bs.put_i64(session_timeout_ms);

    int64_t sid;
    {
        std::lock_guard lock(append_entries_mutex);

        auto result = raft_instance->append_entries({entry});

        if (!result->has_result())
            result->get();

        if (!result->get_accepted())
            throw Exception(ErrorCodes::RAFT_ERROR, "Cannot send session_id request to RAFT, reason {}", result->get_result_str());

        if (result->get_result_code() != nuraft::cmd_result_code::OK)
            throw Exception(ErrorCodes::RAFT_ERROR, "session_id request failed to RAFT");

        auto resp = result->get();
        if (resp == nullptr)
            throw Exception(ErrorCodes::RAFT_ERROR, "Received nullptr as session_id");

        nuraft::buffer_serializer bs_resp(resp);
        sid = bs_resp.get_i64();
    }

    {
        std::unique_lock session_id_lock(new_session_id_callback_mutex);
        if (!state_machine->getStore().containsSession(sid))
        {
            ptr<std::condition_variable> condition = std::make_shared<std::condition_variable>();
            new_session_id_callback.emplace(sid, condition);

            using namespace std::chrono_literals;
            auto status = condition->wait_for(session_id_lock, session_timeout_ms * 1ms);

            new_session_id_callback.erase(sid);
            if (status == std::cv_status::timeout)
            {
                throw Exception(ErrorCodes::RAFT_ERROR, "Time out, can not allocate session {}", sid);
            }
        }
    }

    LOG_DEBUG(log, "Got session {}", sid);
    return sid;
}

bool KeeperServer::updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms)
{
    LOG_DEBUG(log, "Updating session timeout for {}", NumberFormatter::formatHex(session_id, true));

    auto entry = buffer::alloc(sizeof(int64_t) + sizeof(int64_t));
    nuraft::buffer_serializer bs(entry);

    bs.put_i64(session_id);
    bs.put_i64(session_timeout_ms);

    auto result = raft_instance->append_entries({entry});

    if (!result->has_result())
        result->get();

    if (!result->get_accepted())
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot update session timeout, reason {}", result->get_result_str());

    if (result->get_result_code() != nuraft::cmd_result_code::OK)
        throw Exception(ErrorCodes::RAFT_ERROR, "Update session timeout failed to RAFT");

    if (!result->get())
        throw Exception(ErrorCodes::RAFT_ERROR, "Received nullptr when updating session timeout");

    auto buffer = ReadBufferFromNuraftBuffer(*result->get());
    int8_t is_success;
    Coordination::read(is_success, buffer);

    if (!is_success)
        return false;

    {
        std::unique_lock session_id_lock(new_session_id_callback_mutex);

        const auto & dead_session = getDeadSessions();
        if (std::count(dead_session.begin(), dead_session.end(), session_id))
        {
            ptr<std::condition_variable> condition = std::make_shared<std::condition_variable>();

            new_session_id_callback.emplace(session_id, condition);

            using namespace std::chrono_literals;
            auto status = condition->wait_for(session_id_lock, session_timeout_ms * 1ms);

            new_session_id_callback.erase(session_id);
            if (status == std::cv_status::timeout)
            {
                throw Exception(ErrorCodes::RAFT_ERROR, "Time out, can not allocate session {}", session_id);
            }
        }
    }

    return is_success;
}

void KeeperServer::setSessionExpirationTime(int64_t session_id, int64_t expiration_time)
{
    state_machine->getStore().setSessionExpirationTime(session_id, expiration_time);
}

int64_t KeeperServer::getSessionTimeout(int64_t session_id)
{
    LOG_DEBUG(log, "get session timeout for {}", session_id);
    if (state_machine->getStore().session_and_timeout.contains(session_id))
    {
        return state_machine->getStore().session_and_timeout.find(session_id)->second;
    }
    else
    {
        LOG_WARNING(log, "Not found session timeout for {}", session_id);
        return -1;
    }
}

bool KeeperServer::isLeader() const
{
    return raft_instance->is_leader();
}


bool KeeperServer::isObserver() const
{
    auto cluster_config = state_manager->get_cluster_config();
    return cluster_config->get_server(server_id)->is_learner();
}

bool KeeperServer::isFollower() const
{
    return !isLeader() && !isObserver();
}

bool KeeperServer::isLeaderAlive() const
{
    /// nuraft leader_ and role_ not sync
    return raft_instance->is_leader_alive() && raft_instance->get_leader() != -1;
}

uint64_t KeeperServer::getFollowerCount() const
{
    return raft_instance->get_peer_info_all().size();
}

uint64_t KeeperServer::getSyncedFollowerCount() const
{
    uint64_t last_log_idx = raft_instance->get_last_log_idx();
    const auto followers = raft_instance->get_peer_info_all();

    uint64_t stale_followers = 0;

    const uint64_t stale_follower_gap = raft_instance->get_current_params().stale_log_gap_;
    for (const auto & fl : followers)
    {
        if (last_log_idx > fl.last_log_idx_ + stale_follower_gap)
            stale_followers++;
    }
    return followers.size() - stale_followers;
}

nuraft::cb_func::ReturnCode KeeperServer::callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * /* param */)
{
    if (type == nuraft::cb_func::Type::BecomeFresh || type == nuraft::cb_func::Type::BecomeLeader)
    {
        std::unique_lock lock(initialized_mutex);
        initialized_flag = true;
        initialized_cv.notify_all();
    }
    return nuraft::cb_func::ReturnCode::Ok;
}

void KeeperServer::waitInit()
{
    std::unique_lock lock(initialized_mutex);
    int64_t timeout = settings->raft_settings->startup_timeout;
    if (!initialized_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] { return initialized_flag.load(); }))
        throw Exception(ErrorCodes::RAFT_ERROR, "Failed to wait RAFT initialization");
}

std::vector<int64_t> KeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

ConfigUpdateActions KeeperServer::getConfigurationDiff(const Poco::Util::AbstractConfiguration & config_)
{
    return state_manager->getConfigurationDiff(config_);
}

bool KeeperServer::applyConfigurationUpdate(const ConfigUpdateAction & task)
{
    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to add server with id {}", task.server->get_id());
        bool added = false;
        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) != nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully added", task.server->get_id());
                added = true;
                break;
            }

            if (!isLeader())
            {
                LOG_INFO(log, "We are not leader anymore, will not try to add server {}", task.server->get_id());
                break;
            }

            auto result = raft_instance->add_srv(*task.server);
            if (!result->get_accepted())
                LOG_INFO(
                    log,
                    "Command to add server {} was not accepted for the {} time, will sleep for {} ms and retry",
                    task.server->get_id(),
                    i + 1,
                    sleep_ms * (i + 1));

            LOG_DEBUG(log, "Wait for apply action AddServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!added)
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Configuration change to add server (id {}) was not accepted by RAFT after all {} retries",
                task.server->get_id(),
                settings->raft_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::RemoveServer)
    {
        LOG_INFO(log, "Will try to remove server with id {}", task.server->get_id());

        bool removed = false;
        if (task.server->get_id() == state_manager->server_id())
        {
            LOG_INFO(
                log,
                "Trying to remove leader node (ourself), so will yield leadership and some other node (new leader) will try remove us. "
                "Probably you will have to run SYSTEM RELOAD CONFIG on the new leader node");

            raft_instance->yield_leadership();
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * 5));
            return false;
        }

        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) == nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully removed", task.server->get_id());
                removed = true;
                break;
            }

            if (!isLeader())
            {
                LOG_INFO(log, "We are not leader anymore, will not try to remove server {}", task.server->get_id());
                break;
            }

            auto result = raft_instance->remove_srv(task.server->get_id());
            if (!result->get_accepted())
                LOG_INFO(
                    log,
                    "Command to remove server {} was not accepted for the {} time, will sleep for {} ms and retry",
                    task.server->get_id(),
                    i + 1,
                    sleep_ms * (i + 1));

            LOG_DEBUG(log, "Wait for apply action RemoveServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!removed)
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Configuration change to remove server (id {}) was not accepted by RAFT after all {} retries",
                task.server->get_id(),
                settings->raft_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::UpdatePriority)
        raft_instance->set_priority(task.server->get_id(), task.server->get_priority());
    else
        LOG_WARNING(log, "Unknown configuration update type {}", static_cast<uint64_t>(task.action_type));

    return true;
}

bool KeeperServer::waitConfigurationUpdate(const ConfigUpdateAction & task)
{
    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to wait server with id {} to be added", task.server->get_id());
        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) != nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully added by leader", task.server->get_id());
                return true;
            }

            if (isLeader())
            {
                LOG_INFO(log, "We are leader now, probably we will have to add server {}", task.server->get_id());
                return false;
            }

            LOG_DEBUG(log, "Wait for action AddServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        return false;
    }
    else if (task.action_type == ConfigUpdateActionType::RemoveServer)
    {
        LOG_INFO(log, "Will try to wait remove of server with id {}", task.server->get_id());

        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) == nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully removed by leader", task.server->get_id());
                return true;
            }

            if (isLeader())
            {
                LOG_INFO(log, "We are leader now, probably we will have to remove server {}", task.server->get_id());
                return false;
            }

            LOG_DEBUG(log, "Wait for action RemoveServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        return false;
    }
    else if (task.action_type == ConfigUpdateActionType::UpdatePriority)
        return true;
    else
        LOG_WARNING(log, "Unknown configuration update type {}", static_cast<uint64_t>(task.action_type));
    return true;
}

void KeeperServer::reConfigIfNeed()
{
    //    if (!isLeader())
    //        return;
    //
    //    auto cur_cluster_config = state_manager->load_config();
    //
    //    std::vector<String> srvs_removed;
    //    std::vector<ptr<srv_config>> srvs_added;
    //
    //    auto new_cluster_config = state_manager->parseClusterConfig(config, "keeper.cluster");
    //    std::list<ptr<srv_config>> & new_srvs(new_cluster_config->get_servers());
    //    std::list<ptr<srv_config>> & old_srvs(cur_cluster_config->get_servers());
    //
    //    for (auto it = new_srvs.begin(); it != new_srvs.end(); ++it)
    //    {
    //        if (!cur_cluster_config->get_server((*it)->get_id()))
    //            srvs_added.push_back(*it);
    //    }
    //
    //    for (auto it = old_srvs.begin(); it != old_srvs.end(); ++it)
    //    {
    //        if (!new_cluster_config->get_server((*it)->get_id()))
    //            srvs_removed.push_back((*it)->get_endpoint());
    //    }
    //
    //    for (auto & end_point : srvs_removed)
    //    {
    //        removeServer(end_point);
    //    }
    //
    //    for (auto srv_add : srvs_added)
    //    {
    //        addServer(srv_add);
    //    }
}

uint64_t KeeperServer::createSnapshot()
{
    uint64_t log_idx = raft_instance->create_snapshot();
    if (log_idx != 0)
        LOG_INFO(log, "Snapshot creation scheduled with last committed log index {}.", log_idx);
    else
        LOG_WARNING(log, "Failed to schedule snapshot creation task.");
    return log_idx;
}

KeeperLogInfo KeeperServer::getKeeperLogInfo()
{
    KeeperLogInfo log_info;
    auto log_store = state_manager->load_log_store();
    if (log_store)
    {
        log_info.first_log_idx = log_store->start_index();
        log_info.first_log_term = log_store->term_at(log_info.first_log_idx);
    }

    if (raft_instance)
    {
        log_info.last_log_idx = raft_instance->get_last_log_idx();
        log_info.last_log_term = raft_instance->get_last_log_term();
        log_info.last_committed_log_idx = raft_instance->get_committed_log_idx();
        log_info.leader_committed_log_idx = raft_instance->get_leader_committed_log_idx();
        log_info.target_committed_log_idx = raft_instance->get_target_committed_log_idx();
        log_info.last_snapshot_idx = raft_instance->get_last_snapshot_idx();
    }

    return log_info;
}

bool KeeperServer::requestLeader()
{
    return isLeader() || raft_instance->request_leadership();
}

}
