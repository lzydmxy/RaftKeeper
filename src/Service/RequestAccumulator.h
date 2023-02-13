#pragma once

#include <Service/KeeperServer.h>
#include <Service/RequestProcessor.h>
#include <Service/RequestsQueue.h>

namespace RK
{

/** Accumulate requests into a batch to promote performance.
 *
 * Request in a batch must be
 *      1. all write request
 *      2. continuous
 *
 * The batch is transferred to Raft and goes through log replication flow.
 */
class RequestAccumulator
{
    using RequestForSession = KeeperStore::RequestForSession;
    using ThreadPoolPtr = std::shared_ptr<ThreadPool>;
    using NuRaftResult = nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>>;
    using RunnerId = size_t;

public:
    explicit RequestAccumulator(std::shared_ptr<RequestProcessor> request_processor_) : request_processor(request_processor_) { }

    void push(RequestForSession request_for_session);

    bool waitResultAndHandleError(NuRaftResult prev_result, const KeeperStore::RequestsForSessions & prev_batch);

    void run(RunnerId runner_id);

    void shutdown();

    void initialize(
        size_t thread_count,
        std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
        std::shared_ptr<KeeperServer> server_,
        UInt64 operation_timeout_ms_,
        UInt64 max_batch_size_);

private:
    ptr<RequestsQueue> requests_queue;
    ThreadPoolPtr request_thread;

    bool shutdown_called{false};
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

    std::shared_ptr<KeeperServer> server;
    std::shared_ptr<RequestProcessor> request_processor;

    UInt64 operation_timeout_ms;
    UInt64 max_batch_size;
};

}
