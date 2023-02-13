/**
 * Copyright 2016-2023 ClickHouse, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <memory>
#include <optional>
#include <Poco/Event.h>
#include "ZooKeeper.h"
#include "Common.h"

namespace RK
{
    namespace ErrorCodes
    {
    }
}

namespace zkutil
{

/// This class allows querying the contents of ZooKeeper nodes and caching the results.
/// Watches are set for cached nodes and for nodes that were nonexistent at the time of query.
/// After a watch fires, the callback or event that was passed by the user is notified.
///
/// NOTE: methods of this class are not thread-safe.
///
/// Intended use case: if you need one thread to watch changes in several nodes.
/// If instead you use simple a watch event for this, watches will accumulate for nodes that do not change
/// or change rarely.
class ZooKeeperNodeCache
{
public:
    ZooKeeperNodeCache(GetZooKeeper get_zookeeper);

    ZooKeeperNodeCache(const ZooKeeperNodeCache &) = delete;
    ZooKeeperNodeCache(ZooKeeperNodeCache &&) = default;

    struct ZNode
    {
        bool exists = false;
        std::string contents;
        Coordination::Stat stat{};
    };

    ZNode get(const std::string & path, EventPtr watch_event);
    ZNode get(const std::string & path, Coordination::WatchCallback watch_callback);

private:
    GetZooKeeper get_zookeeper;

    struct Context
    {
        std::mutex mutex;
        std::unordered_set<std::string> invalidated_paths;
        bool all_paths_invalidated = false;
    };

    std::shared_ptr<Context> context;

    std::unordered_map<std::string, ZNode> path_to_cached_znode;
};

}
