#pragma once

#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <Coordination/InMemoryLogStore.h>
#include <Coordination/InMemoryStateManager.h>
#include <Coordination/NuKeeperStateMachine.h>
#include <Coordination/NuKeeperStorage.h>
#include <Coordination/CoordinationSettings.h>
#include <unordered_map>

namespace DB
{

class NuKeeperServer
{
private:
    int server_id;

    std::string hostname;

    int port;

    std::string endpoint;

    CoordinationSettingsPtr coordination_settings;

    nuraft::ptr<NuKeeperStateMachine> state_machine;

    nuraft::ptr<nuraft::state_mgr> state_manager;

    nuraft::raft_launcher launcher;

    nuraft::ptr<nuraft::raft_server> raft_instance;

    std::mutex append_entries_mutex;

    ResponsesQueue & responses_queue;

public:
    NuKeeperServer(
        int server_id_, const std::string & hostname_, int port_,
        const CoordinationSettingsPtr & coordination_settings_,
        ResponsesQueue & responses_queue_);

    void startup();

    void putRequest(const NuKeeperStorage::RequestForSession & request);

    int64_t getSessionID(int64_t session_timeout_ms);

    std::unordered_set<int64_t> getDeadSessions();

    void addServer(int server_id_, const std::string & server_uri, bool can_become_leader_, int32_t priority);

    bool isLeader() const;

    bool isLeaderAlive() const;

    bool waitForServer(int32_t server_id) const;
    bool waitForServers(const std::vector<int32_t> & ids) const;
    void waitForCatchUp() const;

    void shutdown();
};

}
