#include "ThreadStatus.h"
#include <common/logger_useful.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadProfileEvents.h>

#include <Poco/Thread.h>
#include <Poco/Ext/ThreadNumber.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PTHREAD_ERROR;
}


/// Order of current_thread and current_thread_scope matters
thread_local ThreadStatusPtr current_thread = ThreadStatus::create();
thread_local ThreadStatus::CurrentThreadScope current_thread_scope;


TasksStatsCounters TasksStatsCounters::current()
{
    TasksStatsCounters res;
    current_thread->taskstats_getter->getStat(res.stat, current_thread->os_thread_id);
    return res;
}


ThreadStatus::ThreadStatus()
{
    thread_number = Poco::ThreadNumber::get();
    os_thread_id = TaskStatsInfoGetter::getCurrentTID();

    last_rusage = std::make_unique<RUsageCounters>();
    last_taskstats = std::make_unique<TasksStatsCounters>();
    taskstats_getter = std::make_unique<TaskStatsInfoGetter>();

    memory_tracker.setDescription("(for thread)");
    log = &Poco::Logger::get("ThreadStatus");

    /// NOTE: It is important not to do any non-trivial actions (like updating ProfileEvents or logging) before ThreadStatus is created
    /// Otherwise it could lead to SIGSEGV due to current_thread dereferencing
}

ThreadStatusPtr ThreadStatus::create()
{
    return ThreadStatusPtr(new ThreadStatus);
}

ThreadStatus::~ThreadStatus() = default;

void ThreadStatus::initPerformanceCounters()
{
    performance_counters_finalized = false;

    /// Clear stats from previous query if a new query is started
    /// TODO: make separate query_thread_performance_counters and thread_performance_counters
    performance_counters.resetCounters();
    memory_tracker.resetCounters();
    memory_tracker.setDescription("(for thread)");

    query_start_time_nanoseconds = getCurrentTimeNanoseconds();
    query_start_time = time(nullptr);
    ++queries_started;

    *last_rusage = RUsageCounters::current(query_start_time_nanoseconds);
    has_permissions_for_taskstats = TaskStatsInfoGetter::checkPermissions();
    if (has_permissions_for_taskstats)
        *last_taskstats = TasksStatsCounters::current();
}

void ThreadStatus::updatePerformanceCounters()
{
    try
    {
        RUsageCounters::updateProfileEvents(*last_rusage, performance_counters);
        if (has_permissions_for_taskstats)
            TasksStatsCounters::updateProfileEvents(*last_taskstats, performance_counters);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void ThreadStatus::assertState(const std::initializer_list<int> & permitted_states, const char * description)
{
    for (auto permitted_state : permitted_states)
    {
        if (getCurrentState() == permitted_state)
            return;
    }

    std::stringstream ss;
    ss << "Unexpected thread state " << getCurrentState();
    if (description)
        ss << ": " << description;
    throw Exception(ss.str(), ErrorCodes::LOGICAL_ERROR);
}

void ThreadStatus::attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue)
{
    logs_queue_ptr = logs_queue;

    if (!thread_group)
        return;

    std::unique_lock lock(thread_group->mutex);
    thread_group->logs_queue_ptr = logs_queue;
}

}
