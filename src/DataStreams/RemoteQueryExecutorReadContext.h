#pragma once

#if defined(OS_LINUX)

#include <sys/epoll.h>
#include <Common/Fiber.h>
#include <Common/FiberStack.h>
#include <Common/TimerDescriptor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_OPEN_FILE;
    extern const int SOCKET_TIMEOUT;
}

class RemoteQueryExecutorReadContext
{
public:
    using Self = RemoteQueryExecutorReadContext;

    bool is_read_in_progress = false;
    Packet packet;

    std::exception_ptr exception;
    FiberStack stack;
    boost::context::fiber fiber;
    std::mutex fiber_lock;
    // std::unique_lock<std::mutex> * connection_lock;

    Poco::Timespan receive_timeout;
    MultiplexedConnections & connections;

    TimerDescriptor timer{CLOCK_MONOTONIC, 0};
    int socket_fd = -1;
    int epoll_fd;
    int pipe_fd[2];

    explicit RemoteQueryExecutorReadContext(MultiplexedConnections & connections_) : connections(connections_)
    {
        epoll_fd = epoll_create(2);
        if (-1 == epoll_fd)
            throwFromErrno("Cannot create epoll descriptor", ErrorCodes::CANNOT_OPEN_FILE);

        if (-1 == pipe2(pipe_fd, O_NONBLOCK))
            throwFromErrno("Cannot create pipe", ErrorCodes::CANNOT_OPEN_FILE);

        {
            epoll_event socket_event;
            socket_event.events = EPOLLIN | EPOLLPRI;
            socket_event.data.fd = pipe_fd[0];

            if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_ADD, pipe_fd[0], &socket_event))
                throwFromErrno("Cannot add pipe descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);
        }

        {
            epoll_event timer_event;
            timer_event.events = EPOLLIN | EPOLLPRI;
            timer_event.data.fd = timer.getDescriptor();

            if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_ADD, timer_event.data.fd, &timer_event))
                throwFromErrno("Cannot add timer descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);
        }

        auto lock = std::make_unique<std::unique_lock<std::mutex>>(connections.cancel_mutex, std::defer_lock);
        auto routine = Routine{connections, *this, std::move(lock)};
        // connection_lock = routine.connection_lock.get();
        fiber = boost::context::fiber(std::allocator_arg_t(), stack, std::move(routine));
    }

    void setSocket(Poco::Net::Socket & socket)
    {
        int fd = socket.impl()->sockfd();
        if (fd == socket_fd)
            return;

        epoll_event socket_event;
        socket_event.events = EPOLLIN | EPOLLPRI;
        socket_event.data.fd = fd;

        if (socket_fd != -1)
        {
            if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_DEL, socket_fd, &socket_event))
                throwFromErrno("Cannot remove socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);
        }

        socket_fd = fd;

        if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &socket_event))
            throwFromErrno("Cannot add socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);

        receive_timeout = socket.impl()->getReceiveTimeout();
    }

    bool checkTimeout() const
    {
        try
        {
            return checkTimeoutImpl();
        }
        catch (DB::Exception & e)
        {
            e.addMessage(" while reading from socket ({})", connections.getSocket().peerAddress().toString());
            throw;
        }
    }

    bool checkTimeoutImpl() const
    {
        epoll_event events[3];
        events[0].data.fd = events[1].data.fd = events[2].data.fd = -1;

        /// Wait for epoll_fd will not block if it was polled externally.
        int num_events = epoll_wait(epoll_fd, events, 3, 0);
        if (num_events == -1)
            throwFromErrno("Failed to epoll_wait", ErrorCodes::CANNOT_READ_FROM_SOCKET);

        bool is_socket_ready = false;
        bool is_pipe_alarmed = false;
        bool has_timer_alarm = false;

        for (int i = 0; i < num_events; ++i)
        {
            if (events[i].data.fd == socket_fd)
                is_socket_ready = true;
            if (events[i].data.fd == timer.getDescriptor())
                has_timer_alarm = true;
            if (events[i].data.fd == pipe_fd[0])
                is_pipe_alarmed = true;
        }

        if (is_pipe_alarmed)
            return false;

        if (has_timer_alarm && !is_socket_ready)
        {
            /// Socket receive timeout. Drain it in case or error, or it may be hide by timeout exception.
            timer.drain();
            throw NetException("Timeout exceeded", ErrorCodes::SOCKET_TIMEOUT);
        }

        return true;
    }

    void setTimer() const
    {
        /// Did not get packet yet. Init timeout for the next async reading.
        timer.reset();

        if (receive_timeout.totalMicroseconds())
            timer.setRelative(receive_timeout);
    }

    bool resumeRoutine()
    {
        if (is_read_in_progress && !checkTimeout())
            return false;

        {
            std::lock_guard guard(fiber_lock);
            if (!fiber)
                return false;

//            if (!connection_lock->owns_lock())
//                connection_lock->lock();

            fiber = std::move(fiber).resume();

//            if (!is_read_in_progress)
//                connection_lock->unlock();
        }

        if (exception)
            std::rethrow_exception(std::move(exception));

        if (is_read_in_progress)
        {
            auto & socket = connections.getSocket();
            try
            {
                setSocket(socket);
            }
            catch (DB::Exception & e)
            {
                e.addMessage(" while reading from socket ({})", socket.peerAddress().toString());
                throw;
            }
        }

        return true;
    }

    void cancel()
    {
        std::lock_guard guard(fiber_lock);
        boost::context::fiber to_destroy = std::move(fiber);

        uint64_t buf = 0;
        while (-1 == write(pipe_fd[1], &buf, sizeof(buf)))
        {
            if (errno == EAGAIN)
                break;

            if (errno != EINTR)
                throwFromErrno("Cannot write to pipe", ErrorCodes::CANNOT_READ_FROM_SOCKET);
        }
    }

    ~RemoteQueryExecutorReadContext()
    {
        /// socket_fd is closed by Poco::Net::Socket
        /// timer_fd is closed by TimerDescriptor
        close(epoll_fd);
    }

    struct Routine
    {
        MultiplexedConnections & connections;
        Self & read_context;
        std::unique_ptr<std::unique_lock<std::mutex>> connection_lock;

        Fiber operator()(Fiber && sink) const
        {
            try
            {
                while (true)
                {
                    read_context.is_read_in_progress = true;
                    read_context.packet = connections.receivePacketUnlocked(&sink);
                    read_context.is_read_in_progress = false;

                    sink = std::move(sink).resume();
                }
            }
            catch (const boost::context::detail::forced_unwind &)
            {
                throw;
            }
            catch (...)
            {
                read_context.exception = std::current_exception();
            }

            return std::move(sink);
        }
    };
};
}
#else
namespace DB
{
class RemoteQueryExecutorReadContext
{
public:
    void cancel() {}
};

}
#endif
