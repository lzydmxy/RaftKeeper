#pragma once

#if defined(OS_LINUX)

#include <variant>

#include <Client/IConnections.h>
#include <Common/FiberStack.h>
#include <Common/Fiber.h>
#include <Common/Epoll.h>
#include <Common/TimerDescriptor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_SOCKET;
}

/// Class for nonblocking packet receiving. It runs connection->receivePacket
/// in fiber and sets special read callback which is called when
/// reading from socket blocks. When read callback is called,
/// socket and receive timeout are added in epoll and execution returns to the main program.
/// So, you can poll this epoll file descriptor to determine when to resume
/// packet receiving.
class PacketReceiver
{
public:
    explicit PacketReceiver(Connection * connection_) : connection(connection_)
    {
        epoll.add(receive_timeout.getDescriptor());
        epoll.add(connection->getSocket()->impl()->sockfd());

        if (-1 == pipe2(pipe_fd, O_NONBLOCK))
            throwFromErrno("Cannot create pipe", ErrorCodes::CANNOT_OPEN_FILE);
        epoll.add(pipe_fd[0]);

        fiber = boost::context::fiber(std::allocator_arg_t(), fiber_stack, Routine{*this});
    }

    ~PacketReceiver()
    {
        close(pipe_fd[0]);
        close(pipe_fd[1]);
    }

    /// Resume packet receiving.
    std::variant<int, Packet, Poco::Timespan> resume()
    {
        /// If there is no pending data, check receive timeout.
        if (!connection->hasReadPendingData() && !checkReceiveTimeout())
        {
            /// Receive timeout expired.
            return Poco::Timespan();
        }

        /// Resume fiber.
        fiber = std::move(fiber).resume();
        if (exception)
            std::rethrow_exception(std::move(exception));

        if (is_read_in_process)
            return epoll.getFileDescriptor();

        /// Write something in pipe when buffer has pending data, because
        /// in this case socket won't be ready in epoll but we need to tell
        /// outside that there is more data in buffer.
        if (connection->hasReadPendingData())
        {
            uint64_t buf = 0;
            while (-1 == write(pipe_fd[1], &buf, sizeof(buf)))
            {
                if (errno == EAGAIN)
                    break;

                if (errno != EINTR)
                    throwFromErrno("Cannot write to pipe", ErrorCodes::CANNOT_READ_FROM_SOCKET);
            }
        }

        /// Receiving packet was finished.
        return std::move(packet);
    }

    void cancel()
    {
        Fiber to_destroy = std::move(fiber);
        connection = nullptr;
    }

    int getFileDescriptor() const { return epoll.getFileDescriptor(); }

private:
    /// When epoll file descriptor is ready, check if it's an expired timeout.
    /// Return false if receive timeout expired and socket is not ready, return true otherwise.
    bool checkReceiveTimeout()
    {
        bool is_socket_ready = false;
        bool is_pipe_ready = false;
        bool is_receive_timeout_expired = false;

        epoll_event events[2];
        events[0].data.fd = events[1].data.fd = -1;
        size_t ready_count = epoll.getManyReady(2, events, true);

        for (size_t i = 0; i != ready_count; ++i)
        {
            if (events[i].data.fd == connection->getSocket()->impl()->sockfd())
                is_socket_ready = true;
            if (events[i].data.fd == pipe_fd[0])
                is_pipe_ready = true;
            if (events[i].data.fd == receive_timeout.getDescriptor())
                is_receive_timeout_expired = true;
        }

        if (is_pipe_ready)
        {
            drainPipe();
            return true;
        }

        if (is_receive_timeout_expired && !is_socket_ready)
        {
            receive_timeout.reset();
            return false;
        }

        return true;
    }

    void drainPipe()
    {
        uint64_t buf;
        while (true)
        {
            ssize_t res = read(pipe_fd[0], &buf, sizeof(buf));
            if (res < 0)
            {
                if (errno == EAGAIN)
                    break;

                if (errno != EINTR)
                    throwFromErrno("Cannot drain pipe_fd", ErrorCodes::CANNOT_READ_FROM_SOCKET);
            }
        }
    }

    struct Routine
    {
        PacketReceiver & receiver;

        struct ReadCallback
        {
            PacketReceiver & receiver;
            Fiber & sink;

            void operator()(int, const Poco::Timespan & timeout, const std::string &)
            {
                receiver.receive_timeout.setRelative(timeout);
                receiver.is_read_in_process = true;
                sink = std::move(sink).resume();
                receiver.is_read_in_process = false;
                receiver.receive_timeout.reset();
            }
        };

        Fiber operator()(Fiber && sink)
        {
            try
            {
                while (true)
                {
                    {
                        AsyncCallbackSetter async_setter(receiver.connection, ReadCallback{receiver, sink});
                        receiver.packet = receiver.connection->receivePacket();
                    }
                    sink = std::move(sink).resume();
                }

            }
            catch (const boost::context::detail::forced_unwind &)
            {
                /// This exception is thrown by fiber implementation in case if fiber is being deleted but hasn't exited
                /// It should not be caught or it will segfault.
                /// Other exceptions must be caught
                throw;
            }
            catch (...)
            {
                receiver.exception = std::current_exception();
            }

            return std::move(sink);
        }
    };

    Connection * connection;
    Packet packet;

    Fiber fiber;
    FiberStack fiber_stack;

    /// We use timer descriptor for checking socket receive timeout.
    TimerDescriptor receive_timeout;

    /// In read callback we add socket file descriptor and timer descriptor with receive timeout
    /// in epoll, so we can return epoll file descriptor outside for polling.
    Epoll epoll;

    /// Pipe is used when there is pending data in buffer
    /// after receiving packet socket won't be ready in epoll in this case),
    /// so we add pipe_fd in epoll and write something in it to tell
    /// outside that we are ready to receive new packet.
    int pipe_fd[2];

    /// If and exception occurred in fiber resume, we save it and rethrow.
    std::exception_ptr exception;

    bool is_read_in_process = false;
};

}
#endif
