#pragma once

#include <thread>
#include <set>
#include <map>
#include <list>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <functional>
#include <Poco/Event.h>
#include <Poco/Timestamp.h>
#include <Core/Types.h>

namespace DB
{

/** Using a fixed number of threads, perform an arbitrary number of tasks in an infinite loop.
  * In this case, one task can run simultaneously from different threads.
  * Designed for tasks that perform continuous background work (for example, merge).
  * `Task` is a function that returns a bool - did it do any work.
  * If not, then the next time will be done later.
  */
class BackgroundProcessingPool
{
public:
    /// Returns true, if some useful work was done. In that case, thread will not sleep before next run of this task.
    using Task = std::function<bool()>;


    class TaskInfo
    {
    public:
        /// Wake up any thread.
        void wake();

        TaskInfo(BackgroundProcessingPool & pool_, const Task & function_) : pool(pool_), function(function_) {}

    private:
        friend class BackgroundProcessingPool;

        BackgroundProcessingPool & pool;
        Task function;

        /// Read lock is hold when task is executed.
        std::shared_mutex rwlock;
        std::atomic<bool> removed {false};

        std::multimap<Poco::Timestamp, std::shared_ptr<TaskInfo>>::iterator iterator;
    };

    using TaskHandle = std::shared_ptr<TaskInfo>;


    BackgroundProcessingPool(int size_);

    size_t getNumberOfThreads() const
    {
        return size;
    }

    TaskHandle addTask(const Task & task);
    void removeTask(const TaskHandle & task);

    ~BackgroundProcessingPool();

private:
    using Tasks = std::multimap<Poco::Timestamp, TaskHandle>;    /// key is desired next time to execute (priority).
    using Threads = std::vector<std::thread>;

    const size_t size;
    static constexpr double sleep_seconds = 10;
    static constexpr double sleep_seconds_random_part = 1.0;

    Tasks tasks;         /// Ordered in priority.
    std::mutex tasks_mutex;

    Threads threads;

    std::atomic<bool> shutdown {false};
    std::condition_variable wake_event;


    void threadFunction();
};

using BackgroundProcessingPoolPtr = std::shared_ptr<BackgroundProcessingPool>;

}
