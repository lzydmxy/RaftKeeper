#pragma once

#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Service/ACLMap.h>
#include <Service/SessionExpiryQueue.h>
#include <Service/ThreadSafeQueue.h>
#include <Service/formatHex.h>
#include <ZooKeeper/IKeeper.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <Poco/Logger.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/IO/Operators.h>
#include <Common/IO/WriteBufferFromString.h>
#include <Common/ThreadPool.h>
#include <common/logger_useful.h>
#include <shared_mutex>

namespace RK
{

struct StoreRequest;
using StoreRequestPtr = std::shared_ptr<StoreRequest>;
using ChildrenSet = std::unordered_set<std::string>;

/**
 * Represent an entry in data tree.
 */
struct KeeperNode
{
    String data;
    uint64_t acl_id = 0; /// 0 -- no ACL by default

    bool is_ephemeral = false;
    bool is_sequential = false;

    Coordination::Stat stat{};
    ChildrenSet children{};

    std::shared_mutex mutex;

    std::shared_ptr<KeeperNode> clone() const
    {
        auto node = std::make_shared<KeeperNode>();
        node->data = data;
        node->acl_id = acl_id;
        node->is_ephemeral = is_ephemeral;
        node->is_sequential = is_sequential;
        node->stat = stat;
        node->children = children;
        return node;
    }

    /// All stat for client should be generated by this function.
    /// This method will remove numChildren from persisted stat.
    Coordination::Stat statForResponse() const
    {
        Coordination::Stat stat_view;
        stat_view = stat;
        stat_view.numChildren = children.size();
        stat_view.cversion = stat.cversion * 2 - stat.numChildren;
        return stat_view;
    }

    bool operator==(const KeeperNode & rhs) const
    {
        return data == rhs.data && acl_id == rhs.acl_id && is_ephemeral == rhs.is_ephemeral && is_sequential == rhs.is_sequential
            && children == rhs.children;
    }
    bool operator!=(const KeeperNode & rhs) const { return !(rhs == *this); }
};

/// Map for data tree, it is thread safe with read write lock.
/// ConcurrentMap is a two-level unordered_map which is designed
/// to reduce latency for unordered_map scales.
template <typename Element, unsigned NumBlocks>
class ConcurrentMap
{
public:
    using SharedElement = std::shared_ptr<Element>;
    using ElementMap = std::unordered_map<std::string, SharedElement>;
    using Action = std::function<void(const String &, const SharedElement &)>;

    /// Simple encapsulate unordered_map with lock.
    class InnerMap
    {
    public:
        SharedElement get(std::string const & key)
        {
            std::shared_lock lock(mut_);
            auto i = map_.find(key);
            return (i != map_.end()) ? i->second : nullptr;
        }
        bool emplace(std::string const & key, SharedElement && value)
        {
            std::unique_lock lock(mut_);
            auto [_, created] = map_.insert_or_assign(key, std::move(value));
            return created;
        }
        bool emplace(std::string const & key, const SharedElement & value)
        {
            std::unique_lock lock(mut_);
            auto [_, created] = map_.insert_or_assign(key, value);
            return created;
        }
        bool erase(std::string const & key)
        {
            std::unique_lock write_lock(mut_);
            return map_.erase(key);
        }

        size_t size() const
        {
            std::shared_lock lock(mut_);
            return map_.size();
        }

        void forEach(const Action & fn)
        {
            std::shared_lock read_lock(mut_);
            for (auto [key, value] : map_)
                fn(key, value);
        }

        std::shared_mutex & getMutex() { return mut_; }

        /// This method will destroy InnerMap thread safety property.
        /// deprecated use forEach instead.
        ElementMap & getMap() { return map_; }

    private:
        mutable std::shared_mutex mut_;
        ElementMap map_;
    };

private:
    std::array<InnerMap, NumBlocks> maps_;
    std::hash<std::string> hash_;

public:
    SharedElement get(const std::string & key) { return mapFor(key).get(key); }
    SharedElement at(const std::string & key) { return mapFor(key).get(key); }

    bool emplace(const std::string & key, SharedElement && value) { return mapFor(key).emplace(key, std::move(value)); }
    bool emplace(const std::string & key, const SharedElement & value) { return mapFor(key).emplace(key, value); }
    size_t count(const std::string & key) { return get(key) != nullptr ? 1 : 0; }
    bool erase(std::string const & key) { return mapFor(key).erase(key); }

    InnerMap & mapFor(std::string const & key) { return maps_[hash_(key) % NumBlocks]; }
    UInt32 getBlockNum() const { return NumBlocks; }
    InnerMap & getMap(const UInt32 & index) { return maps_[index]; }

    size_t size() const
    {
        size_t s(0);
        for (auto it = maps_.begin(); it != maps_.end(); it++)
        {
            s += it->size();
        }
        return s;
    }
};

/// KeeperStore hold data tree and sessions. It is under statemachine.
class KeeperStore
{
public:
    /// block num for ConcurrentMap
    static constexpr int MAP_BLOCK_NUM = 16;

    /// Attached session id to response
    struct ResponseForSession
    {
        int64_t session_id;
        Coordination::ZooKeeperResponsePtr response;
    };

    using ResponsesForSessions = std::vector<ResponseForSession>;
    using KeeperResponsesQueue = ThreadSafeQueue<KeeperStore::ResponseForSession>;

    /// Attached session id to request
    struct RequestForSession
    {
        int64_t session_id;
        Coordination::ZooKeeperRequestPtr request;

        /// measured in millisecond
        int64_t create_time{};

        /// for forward request
        int32_t server_id{-1};
        int32_t client_id{-1};

        explicit RequestForSession() = default;

        RequestForSession(Coordination::ZooKeeperRequestPtr request_, int64_t session_id_, int64_t create_time_)
            : session_id(session_id_), request(request_), create_time(create_time_)
        {
        }

        bool isForwardRequest() const { return server_id > -1 && client_id > -1; }
    };

    using SessionAndAuth = std::unordered_map<int64_t, Coordination::AuthIDs>;
    using RequestsForSessions = std::vector<RequestForSession>;
    using Container = ConcurrentMap<KeeperNode, MAP_BLOCK_NUM>;

    using Ephemerals = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using EphemeralsPtr = std::shared_ptr<Ephemerals>;
    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionAndWatcherPtr = std::shared_ptr<SessionAndWatcher>;
    using SessionAndTimeout = std::unordered_map<int64_t, int64_t>;
    using SessionIDs = std::vector<int64_t>;

    using Watches = std::map<String /* path, relative of root_path */, SessionIDs>;

    /// global session id counter, used to allocate new session id.
    /// It should be same across all nodes.
    int64_t session_id_counter{1};

    mutable std::shared_mutex auth_mutex;
    SessionAndAuth session_and_auth;

    /// data tree
    Container container;

    /// all ephemerals nodes goes here
    Ephemerals ephemerals;
    mutable std::mutex ephemerals_mutex;

    /// Hold session and expiry time
    SessionExpiryQueue session_expiry_queue;

    /// Hold session and initialized expiry timeout
    SessionAndTimeout session_and_timeout;
    mutable std::mutex session_mutex;

    /// watches information

    /// Session id -> node patch
    SessionAndWatcher sessions_and_watchers;
    /// Node path -> session id. Watches for 'get' and 'exist' requests
    Watches watches;
    /// Node path -> session id. Watches for 'list' request (watches on children).
    Watches list_watches;

    mutable std::mutex watch_mutex;

    /// ACLMap for more compact ACLs storage inside nodes.
    ACLMap acl_map;

    /// Global transaction id, only write request will consume zxid.
    /// It should be same across all nodes.
    std::atomic<int64_t> zxid{0};

    /// finalized flag
    bool finalized{false};

    const String super_digest;

    void clearDeadWatches(int64_t session_id);

    /// increase zxid
    int64_t getZXID() { return zxid++; }

    explicit KeeperStore(int64_t tick_time_ms, const String & super_digest_ = "");

    /// Allocate a new session id with initialized expiry timeout session_timeout_ms.
    /// Will increase session_id_counter and zxid.
    int64_t getSessionID(int64_t session_timeout_ms)
    {
        /// Creating session should increase zxid
        getZXID();

        std::lock_guard lock(session_mutex);
        auto result = session_id_counter++;
        auto it = session_and_timeout.emplace(result, session_timeout_ms);
        if (!it.second)
        {
            LOG_DEBUG(log, "Session {} already exist, must applying a fuzzy log.", toHexString(result));
        }
        session_expiry_queue.addNewSessionOrUpdate(result, session_timeout_ms);
        return result;
    }

    [[maybe_unused]] int64_t getSessionIDCounter() const
    {
        std::lock_guard lock(session_mutex);
        return session_id_counter;
    }

    bool updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms);

    /// process request
    void processRequest(
        ThreadSafeQueue<ResponseForSession> & responses_queue,
        const RequestForSession & request_for_session,
        std::optional<int64_t> new_last_zxid = {},
        bool check_acl = true,
        bool ignore_response = false);

    /// build path children after load data from snapshot
    void buildPathChildren(bool from_zk_snapshot = false);

    void finalize();

    /// Add session id. Used when restoring KeeperStorage from snapshot.
    void addSessionID(int64_t session_id, int64_t session_timeout_ms)
    {
        std::lock_guard lock(session_mutex);
        session_and_timeout.emplace(session_id, session_timeout_ms);
        session_expiry_queue.addNewSessionOrUpdate(session_id, session_timeout_ms);
    }

    std::vector<int64_t> getDeadSessions()
    {
        std::lock_guard lock(session_mutex);
        auto ret = session_expiry_queue.getExpiredSessions();
        return ret;
    }

    std::unordered_map<int64_t, int64_t> sessionToExpirationTime()
    {
        std::lock_guard lock(session_mutex);
        const auto & ret = session_expiry_queue.sessionToExpirationTime();
        return ret;
    }

    void handleRemoteSession(int64_t session_id, int64_t expiration_time)
    {
        std::lock_guard lock(session_mutex);
        session_expiry_queue.setSessionExpirationTime(session_id, expiration_time);
    }

    bool containsSession(int64_t session_id) const;

    /// Introspection functions mostly used in 4-letter commands
    uint64_t getNodesCount() const { return container.size(); }

    uint64_t getApproximateDataSize() const
    {
        UInt64 node_count = container.size();
        UInt64 size_bytes = container.getBlockNum() * sizeof(Container::InnerMap) /* Inner map size */
            + node_count * 8 / 0.75 /*hash map array size*/
            + node_count * sizeof(KeeperNode) /*node size*/
            + node_count * 100; /*path and child of node size*/
        return size_bytes;
    }

    uint64_t getTotalWatchesCount() const;

    uint64_t getWatchedPathsCount() const
    {
        std::lock_guard lock(watch_mutex);
        return watches.size() + list_watches.size();
    }

    uint64_t getSessionsWithWatchesCount() const;

    uint64_t getSessionWithEphemeralNodesCount() const
    {
        std::lock_guard lock(watch_mutex);
        return ephemerals.size();
    }
    uint64_t getTotalEphemeralNodesCount() const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;

private:
    Poco::Logger * log;
};

}
