#pragma once

#include <Poco/Notification.h>
#include <Poco/NotificationQueue.h>
#include <Poco/Timestamp.h>

#include <thread>
#include <atomic>
#include <mutex>
#include <vector>
#include <map>
#include <functional>
#include <boost/noncopyable.hpp>


namespace DB
{

class TaskNotification;

class BackgroundSchedulePool
{
public:
    class TaskInfo;
    using TaskHandle = std::shared_ptr<TaskInfo>;
    using Tasks = std::multimap<Poco::Timestamp, TaskHandle>;
    using Task = std::function<void()>;

    class TaskInfo : public std::enable_shared_from_this<TaskInfo>, private boost::noncopyable
    {
    public:
        TaskInfo(BackgroundSchedulePool & pool, const std::string & name, const Task & function);

        bool schedule();
        bool scheduleAfter(size_t ms);
        bool removed() const { return removed_; }
        bool pause(bool value);

    private:
        using Guard = std::lock_guard<std::recursive_mutex>;

        friend class TaskNotification;
        friend class BackgroundSchedulePool;

        void invalidate();
        void execute();

        std::recursive_mutex lock_;
        std::atomic<bool> removed_ {false};
        std::string name_;
        bool scheduled_ = false;
        BackgroundSchedulePool & pool_;
        Task function_;
        Tasks::iterator iterator_;
    };

    BackgroundSchedulePool(size_t size);
    ~BackgroundSchedulePool();

    TaskHandle addTask(const std::string & name, const Task & task);
    void removeTask(const TaskHandle & task);
    size_t getNumberOfThreads() const { return size_;}

private:
    using Threads = std::vector<std::thread>;

    void threadFunction();
    void delayExecutionThreadFunction();
    void scheduleDelayedTask(const TaskHandle & task, size_t ms);
    void cancelDelayedTask(const TaskHandle & task);

    const size_t size_;
    std::atomic<bool> shutdown_ {false};
    Threads threads_;
    Poco::NotificationQueue queue_;

    // delayed notifications

    std::condition_variable wake_event_;
    std::mutex delayed_tasks_lock_;
    std::thread delayed_thread_;
    Tasks delayed_tasks_;
};

using BackgroundSchedulePoolPtr = std::shared_ptr<BackgroundSchedulePool>;

}
