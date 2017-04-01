#pragma once

#include <atomic>
#include <common/Common.h>
#include <DB/Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric MemoryTracking;
}


/** Tracks memory consumption.
  * It throws an exception if amount of consumed memory become greater than certain limit.
  * The same memory tracker could be simultaneously used in different threads.
  */
class MemoryTracker
{
    std::atomic<Int64> amount {0};
    std::atomic<Int64> peak {0};
    std::atomic<Int64> limit {0};

    /// To test exception safety of calling code, memory tracker throws an exception on each memory allocation with specified probability.
    double fault_probability = 0;

    /// Singly-linked list. All information will be passed to subsequent memory trackers also (it allows to implement trackers hierarchy).
    /// In terms of tree nodes it is the list of parents. Lifetime of these trackers should "include" lifetime of current tracker.
    MemoryTracker * next = nullptr;

    /// You could specify custom metric to track memory usage.
    CurrentMetrics::Metric metric = CurrentMetrics::MemoryTracking;

    /// This description will be used as prefix into log messages (if isn't nullptr)
    const char * description = nullptr;

public:
    MemoryTracker() {}
    MemoryTracker(Int64 limit_) : limit(limit_) {}

    ~MemoryTracker();

    /** Call the following functions before calling of corresponding operations with memory allocators.
      */
    void alloc(Int64 size);

    void realloc(Int64 old_size, Int64 new_size)
    {
        alloc(new_size - old_size);
    }

    /** This function should be called after memory deallocation.
      */
    void free(Int64 size);

    Int64 get() const
    {
        return amount.load(std::memory_order_relaxed);
    }

    Int64 getPeak() const
    {
        return peak.load(std::memory_order_relaxed);
    }

    void setLimit(Int64 limit_)
    {
        limit.store(limit_, std::memory_order_relaxed);
    }

    /** Set limit if it was not set.
      * Otherwise, set limit to new value, if new value is greater than previous limit.
      */
    void setOrRaiseLimit(Int64 value);

    void setFaultProbability(double value)
    {
        fault_probability = value;
    }

    void setNext(MemoryTracker * elem)
    {
        next = elem;
    }

    /// The memory consumption could be shown in realtime via CurrentMetrics counter
    void setMetric(CurrentMetrics::Metric metric_)
    {
        metric = metric_;
    }

    void setDescription(const char * description_)
    {
        description = description_;
    }

    /// Reset the accumulated data.
    void reset();

    /// Prints info about peak memory consumption into log.
    void logPeakMemoryUsage() const;
};


/** Объект MemoryTracker довольно трудно протащить во все места, где выделяются существенные объёмы памяти.
  * Поэтому, используется thread-local указатель на используемый MemoryTracker или nullptr, если его не нужно использовать.
  * Этот указатель выставляется, когда в данном потоке следует отслеживать потребление памяти.
  * Таким образом, его нужно всего-лишь протащить во все потоки, в которых обрабатывается один запрос.
  */
extern __thread MemoryTracker * current_memory_tracker;


#include <boost/noncopyable.hpp>

struct TemporarilyDisableMemoryTracker : private boost::noncopyable
{
    MemoryTracker * memory_tracker;

    TemporarilyDisableMemoryTracker()
    {
        memory_tracker = current_memory_tracker;
        current_memory_tracker = nullptr;
    }

    ~TemporarilyDisableMemoryTracker()
    {
        current_memory_tracker = memory_tracker;
    }
};
