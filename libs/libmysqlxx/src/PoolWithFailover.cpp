#include <Poco/NumberFormatter.h>
#include <mysqlxx/PoolWithFailover.h>


using namespace mysqlxx;

PoolWithFailover::PoolWithFailover(const Poco::Util::AbstractConfiguration & cfg,
                                   const std::string & config_name, const unsigned default_connections,
                                   const unsigned max_connections, const size_t max_tries)
    : max_tries(max_tries)
{
    if (cfg.has(config_name + ".replica"))
    {
        Poco::Util::AbstractConfiguration::Keys replica_keys;
        cfg.keys(config_name, replica_keys);
        for (const auto & replica_config_key : replica_keys)
        {
            if (replica_config_key == "replica")    /// There could be another elements in the same level in configuration file.
            {
                std::string replica_name = config_name + "." + replica_config_key;

                int priority = cfg.getInt(replica_name + ".priority", 0);

                replicas_by_priority[priority].emplace_back(
                    std::make_shared<Pool>(cfg, replica_name, default_connections, max_connections, config_name.c_str()),
                    priority);
            }
        }
    }
    else
    {
        replicas_by_priority[0].emplace_back(
            std::make_shared<Pool>(cfg, config_name, default_connections, max_connections),
            0);
    }
}

PoolWithFailover::PoolWithFailover(const std::string & config_name, const unsigned default_connections,
    const unsigned max_connections, const size_t max_tries)
    : PoolWithFailover{
        Poco::Util::Application::instance().config(), config_name,
        default_connections, max_connections, max_tries}
{}

PoolWithFailover::PoolWithFailover(const PoolWithFailover & other)
    : max_tries{other.max_tries}
{
    for (const auto & replica_with_priority : other.replicas_by_priority)
    {
        Replicas replicas;
        replicas.reserve(replica_with_priority.second.size());
        for (const auto & replica : replica_with_priority.second)
            replicas.emplace_back(std::make_shared<Pool>(*replica.pool), replica.priority);
        replicas_by_priority.emplace(replica_with_priority.first, std::move(replicas));
    }
}

PoolWithFailover::Entry PoolWithFailover::Get()
{
    std::lock_guard<std::mutex> locker(mutex);
    Poco::Util::Application & app = Poco::Util::Application::instance();

    /// If we cannot connect to some replica due to pool overflow, than we will wait and connect.
    Replica * full_pool = nullptr;

    for (size_t try_no = 0; try_no < max_tries; ++try_no)
    {
        full_pool = nullptr;

        for (auto & priority_replicas : replicas_by_priority)
        {
            Replicas & replicas = priority_replicas.second;
            for (size_t i = 0, size = replicas.size(); i < size; ++i)
            {
                Replica & replica = replicas[i];

                try
                {
                    Entry entry = replica.pool->tryGet();

                    if (!entry.isNull())
                    {
                        /// Move all traversed replicas to the end of queue.
                        /// (No need to move replicas with another priority)
                        std::rotate(replicas.begin(), replicas.begin() + i + 1, replicas.end());

                        return entry;
                    }
                }
                catch (const Poco::Exception & e)
                {
                    if (e.displayText() == "mysqlxx::Pool is full") /// NOTE: String comparison is trashy code.
                    {
                        full_pool = &replica;
                    }

                    app.logger().warning("Connection to " + replica.pool->getDescription() + " failed: " + e.displayText());
                    continue;
                }

                app.logger().warning("Connection to " + replica.pool->getDescription() + " failed.");
            }
        }

        app.logger().error("Connection to all replicas failed " + Poco::NumberFormatter::format(try_no + 1) + " times");
    }

    if (full_pool)
    {
        app.logger().error("All connections failed, trying to wait on a full pool " + full_pool->pool->getDescription());
        return full_pool->pool->Get();
    }

    std::stringstream message;
    message << "Connections to all replicas failed: ";
    for (ReplicasByPriority::const_iterator it = replicas_by_priority.begin(); it != replicas_by_priority.end(); ++it)
        for (Replicas::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt)
            message << (it == replicas_by_priority.begin() && jt == it->second.begin() ? "" : ", ") << jt->pool->getDescription();

    throw Poco::Exception(message.str());
}
