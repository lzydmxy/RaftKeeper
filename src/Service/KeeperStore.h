#pragma once

#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Service/ACLMap.h>
#include <Service/SessionExpiryQueue.h>
#include <Service/ThreadSafeQueue.h>
#include <Service/KeeperCommon.h>
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
using ChildrenSet = std::unordered_set<String>;
using Edge = std::pair<String, String>;
using Edges = std::vector<Edge>;

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

    std::shared_ptr<KeeperNode> cloneForSnapshot() const
    {
        auto node = std::make_shared<KeeperNode>();
        node->data = data;
        node->acl_id = acl_id;
        node->is_ephemeral = is_ephemeral;
        node->is_sequential = is_sequential;
        node->stat = stat;
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

struct KeeperNodeWithPath
{
    String path;
    std::shared_ptr<KeeperNode> node;
};

/// KeeperNodeMap is a two-level unordered_map which is designed to reduce latency for unordered_map scaling.
/// It is not a thread-safe map. But it is accessed only in the request processor thread.
template <typename Value, unsigned NumBuckets>
class KeeperNodeMap
{
public:
    using Key = String;
    using ValuePtr = std::shared_ptr<Value>;
    using NestedMap = std::unordered_map<String, ValuePtr>;
    using Action = std::function<void(const String &, const ValuePtr &)>;

    class InnerMap
    {
    public:
        ValuePtr get(const String & key)
        {
            auto i = map.find(key);
            return (i != map.end()) ? i->second : nullptr;
        }

        template <typename T>
        bool emplace(const String & key, T && value)
        {
            return map.insert_or_assign(key, value).second;
        }

        bool erase(const String & key)
        {
            return map.erase(key);
        }

        size_t size() const
        {
            return map.size();
        }

        void clear()
        {
            map.clear();
        }

        void forEach(const Action & fn)
        {
            for (const auto & [key, value] : map)
                fn(key, value);
        }

        /// This method will destroy InnerMap thread safety property.
        /// Deprecated, please use forEach instead.
        NestedMap & getMap() { return map; }

    private:
        NestedMap map;
    };

private:
    inline InnerMap & mapFor(const String & key) { return buckets[hash(key) % NumBuckets]; }

    std::array<InnerMap, NumBuckets> buckets;
    std::hash<String> hash;
    std::atomic<size_t> node_count{0};

public:
    ValuePtr get(const String & key) { return mapFor(key).get(key); }
    ValuePtr at(const String & key) { return mapFor(key).get(key); }

    template <typename T>
    bool emplace(const String & key, T && value)
    {
        if (mapFor(key).emplace(key, std::forward<T>(value)))
        {
            node_count++;
            return true;
        }
        return false;
    }

    template <typename T>
    bool emplace(const String & key, T && value, UInt32 bucket_id)
    {
        if (buckets[bucket_id].emplace(key, std::forward<T>(value)))
        {
            node_count++;
            return true;
        }
        return false;
    }

    bool erase(String const & key)
    {
        if (mapFor(key).erase(key))
        {
            node_count--;
            return true;
        }
        return false;
    }

    size_t count(const String & key) { return get(key) != nullptr ? 1 : 0; }

    UInt32 getBucketIndex(const String & key) { return hash(key) % NumBuckets; }
    UInt32 getBucketNum() const { return NumBuckets; }

    InnerMap & getMap(const UInt32 & bucket_id) { return buckets[bucket_id]; }

    void clear()
    {
        for (auto & bucket : buckets)
            bucket.clear();
        node_count.store(0);
    }

    size_t size() const
    {
        return node_count.load();
    }
};

/// KeeperStore hold data tree and sessions. It is under state machine.
class KeeperStore
{
public:
    /// bucket num for KeeperNodeMap
    static constexpr int MAP_BUCKET_NUM = 16;
    using Container = KeeperNodeMap<KeeperNode, MAP_BUCKET_NUM>;

    using ResponsesForSessions = std::vector<ResponseForSession>;
    using KeeperResponsesQueue = ThreadSafeQueue<ResponseForSession>;

    using SessionAndAuth = std::unordered_map<int64_t, Coordination::AuthIDs>;
    using RequestsForSessions = std::vector<RequestForSession>;

    using Ephemerals = std::unordered_map<int64_t, std::unordered_set<String>>;
    using EphemeralsPtr = std::shared_ptr<Ephemerals>;
    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_set<String>>;
    using SessionAndWatcherPtr = std::shared_ptr<SessionAndWatcher>;
    using SessionAndTimeout = std::unordered_map<int64_t, int64_t>;
    using SessionIDs = std::vector<int64_t>;

    using Watches = std::unordered_map<String /* path, relative of root_path */, SessionIDs>;

    /// Hold Edges in different Buckets based on the parent node's bucket number.
    /// It should be used when load snapshot to built node's childrenSet in parallel without lock.
    using BucketEdges = std::array<Edges, MAP_BUCKET_NUM>;
    using BucketNodes = std::array<std::vector<std::pair<String, std::shared_ptr<KeeperNode>>>, MAP_BUCKET_NUM>;

    /// global session id counter, used to allocate new session id.
    /// It should be same across all nodes.
    int64_t session_id_counter{1};

    mutable std::shared_mutex auth_mutex;
    SessionAndAuth session_and_auth;

    /// data tree
    Container container;

    /// all ephemeral nodes goes here
    Ephemerals ephemerals;
    mutable std::mutex ephemerals_mutex;

    /// Hold session and expiry time
    /// For leader, holds all sessions in cluster.
    /// For follower/leaner, holds only local sessions
    SessionExpiryQueue session_expiry_queue;

    /// Hold session and initialized expiry timeout, only local sessions.
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

    explicit KeeperStore(int64_t tick_time_ms, const String & super_digest_ = "");

    /// Allocate a new session id with initialized expiry timeout session_timeout_ms.
    /// Will increase session_id_counter and zxid.
    int64_t getSessionID(int64_t session_timeout_ms)  /// TODO delete
    {
        /// Creating session should increase zxid
        fetchAndGetZxid();

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

    int64_t getSessionIDCounter() const
    {
        std::lock_guard lock(session_mutex);
        return session_id_counter;
    }

    int64_t getZxid() const
    {
        return zxid.load();
    }

    int64_t getSessionCount() const
    {
        std::lock_guard lock(session_mutex);
        return session_and_timeout.size();
    }

    SessionAndTimeout getSessionTimeOut() const
    {
        std::lock_guard lock(session_mutex);
        return session_and_timeout;
    }

    SessionAndAuth getSessionAuth() const
    {
        std::lock_guard lock(session_mutex);
        return session_and_auth;
    }


    bool updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms); /// TODO delete

    /// process request
    void processRequest(
        ThreadSafeQueue<ResponseForSession> & responses_queue,
        const RequestForSession & request_for_session,
        std::optional<int64_t> new_last_zxid = {},
        bool check_acl = true,
        bool ignore_response = false);

    /// Build children set after loading data from snapshot
    void buildChildrenSet(bool from_zk_snapshot = false);

    // Build children set for the nodes in specified bucket after load data from snapshot.
    void buildBucketChildren(const std::vector<BucketEdges> & all_objects_edges, UInt32 bucket_id);
    void fillDataTreeBucket(const std::vector<BucketNodes> & all_objects_nodes, UInt32 bucket_id);


    void finalize();

    /// Add session id. Used when restoring KeeperStore from snapshot.
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
        UInt64 size_bytes = container.getBucketNum() * sizeof(Container::InnerMap) /* Inner map size */
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
        std::lock_guard lock(ephemerals_mutex);
        return ephemerals.size();
    }
    uint64_t getTotalEphemeralNodesCount() const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;

    /// clear whole store and set to initial state.
    void reset();

    BucketNodes dumpDataTree()
    {
        auto result = BucketNodes();
        ThreadPool object_thread_pool(MAP_BUCKET_NUM);
        for (UInt32 thread_idx = 0; thread_idx < MAP_BUCKET_NUM; thread_idx++)
        {
            object_thread_pool.trySchedule(
                [thread_idx, this, &result]
                {
                    for (UInt32 bucket_idx = 0; bucket_idx < MAP_BUCKET_NUM; bucket_idx++)
                    {
                        if (bucket_idx % MAP_BUCKET_NUM != thread_idx)
                            continue;
                        LOG_WARNING(log, "Dump datatree index {}", bucket_idx);
                        auto bucket = this->container.getMap(bucket_idx).getMap();
                        result[bucket_idx].reserve(bucket.size());
                        size_t key_size = 0;
                        for (auto && [path, node] : bucket)
                        {
                            key_size += path.size();
                            result[bucket_idx].emplace_back(path, node->cloneForSnapshot());
                        }
                        LOG_WARNING(log, "Dump datatree done index {}, key_size {}, result size {}", bucket_idx, key_size, result[bucket_idx].size());
                    }
                });
        }


        object_thread_pool.wait();
        return result;
    }

private:
    int64_t fetchAndGetZxid() { return zxid++; }

    void cleanDeadWatches(int64_t session_id);
    void cleanEphemeralNodes(int64_t session_id, ThreadSafeQueue<ResponseForSession> & responses_queue, bool ignore_response);

    Poco::Logger * log;
};

}
