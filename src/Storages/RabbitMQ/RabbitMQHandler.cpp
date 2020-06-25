#include <common/logger_useful.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{

enum
{
    Lock_timeout = 50,
    Loop_stop_timeout = 200
};


RabbitMQHandler::RabbitMQHandler(uv_loop_t * loop_, Poco::Logger * log_) :
    AMQP::LibUvHandler(loop_),
    loop(loop_),
    log(log_)
{
    tv.tv_sec = 0;
    tv.tv_usec = Loop_stop_timeout;
}


void RabbitMQHandler::onError(AMQP::TcpConnection * connection, const char * message)
{
    LOG_ERROR(log, "Library error report: {}", message);

    if (!connection->usable() || !connection->ready())
    {
        LOG_ERROR(log, "Connection lost completely");
    }

    stop();
}


void RabbitMQHandler::startConsumerLoop(std::atomic<bool> & loop_started)
{
    /* The object of this class is shared between concurrent consumers (who share the same connection == share the same
     * event loop and handler). But the loop should not be attempted to start if it is already running.
     */
    bool expected = false;
    if (loop_started.compare_exchange_strong(expected, true))
    {
        std::lock_guard lock(mutex_before_event_loop);
        stop_scheduled = false;

        uv_run(loop, UV_RUN_NOWAIT);
        loop_started.store(false);
    }
}


void RabbitMQHandler::startProducerLoop()
{
    uv_run(loop, UV_RUN_NOWAIT);
}


void RabbitMQHandler::stop()
{
    std::lock_guard lock(mutex_before_loop_stop);
    uv_stop(loop);
}


void RabbitMQHandler::stopWithTimeout()
{
    stop_scheduled = true;
    uv_stop(loop);
}

}
