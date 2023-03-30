#pragma once

#include <chrono>
#include <Service/KeeperServer.h>
#include <Service/RequestProcessor.h>
#include <Service/RequestsQueue.h>
#include <Common/Stopwatch.h>
#include <Service/Types.h>
#include <Service/ForwardRequest.h>
#include <Service/ForwardResponse.h>

namespace RK
{

using clock = std::chrono::steady_clock;

class RequestForwarder
{
public:
    explicit RequestForwarder(std::shared_ptr<RequestProcessor> request_processor_)
        : request_processor(request_processor_), log(&Poco::Logger::get("RequestForwarder"))
    {
    }

    void push(RequestForSession request_for_session);

    void runSend(RunnerId runner_id);

    void shutdown();

    void runReceive(RunnerId runner_id);

    void initialize(
        size_t thread_count_,
        std::shared_ptr<KeeperServer> server_,
        std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
        UInt64 session_sync_period_ms_);


public:
    std::shared_ptr<RequestProcessor> request_processor;

    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

private:

    void runSessionSync(RunnerId runner_id);
    void runSessionSyncReceive(RunnerId runner_id);

    void processResponse(RunnerId runner_id, ForwardResponsePtr forward_response_ptr);

    void pushToQueue(RunnerId runner_id, ForwardRequestPtr forward_request);

    void removeFromQueue(RunnerId runner_id, ForwardResponsePtr forward_response_ptr);

    bool processTimeoutRequest(RunnerId runner_id, ForwardRequestPtr newFront);

    size_t thread_count;

    ptr<RequestsQueue> requests_queue;

    Poco::Logger * log;

    ThreadPoolPtr request_thread;

    ThreadPoolPtr response_thread;

    ThreadFromGlobalPool session_sync_thread;

    bool shutdown_called{false};

    std::shared_ptr<KeeperServer> server;

    UInt64 session_sync_period_ms = 500;

    std::atomic<UInt8> session_sync_idx{0};

    Stopwatch session_sync_time_watch;

    using ForwardingQueue = ThreadSafeQueue<ForwardRequestPtr, std::list<ForwardRequestPtr>>;
    using ForwardingQueuePtr = std::unique_ptr<ForwardingQueue>;
    std::vector<ForwardingQueuePtr> forwarding_queues;

    Poco::Timespan operation_timeout;

    using ConnectionPool = std::vector<ptr<ForwardingConnection>>;
    std::unordered_map<UInt32, ConnectionPool> connections;
};

}
