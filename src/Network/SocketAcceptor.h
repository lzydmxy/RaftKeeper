/**
* Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH. and Contributors.
* SPDX-License-Identifier:	BSL-1.0
*/
#pragma once

#include <vector>

#include <Poco/Environment.h>
#include <Poco/NObserver.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>

#include <Network/Observer.h>
#include <Network/SocketNotification.h>
#include <Network/SocketReactor.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <common/logger_useful.h>

#include <Service/Context.h>


namespace RK
{

namespace ErrorCodes
{
    extern const int EPOLL_ERROR;
}

using Poco::Net::ServerSocket;
using Poco::Net::Socket;
using Poco::Net::StreamSocket;

/// This class implements the Acceptor part of the Acceptor-Connector design pattern.
///
/// This is a multi-threaded version of SocketAcceptor, it differs from the
/// single-threaded version in number of getWorkerReactors (defaulting to number of processors)
/// that can be specified at construction time and is rotated in a round-robin fashion
/// by event handler. See ParallelSocketAcceptor::onAccept and
/// ParallelSocketAcceptor::createServiceHandler documentation and implementation for
/// details.
template <class ServiceHandler>
class SocketAcceptor
{
public:
    using SocketConfigurator = std::function<void(StreamSocket &)>;
    using MainReactorPtr = AsyncSocketReactorPtr;
    using WorkerReactor = AsyncSocketReactor;
    using WorkerReactorPtr = AsyncSocketReactorPtr;
    using WorkerReactors = std::vector<WorkerReactorPtr>;
    using AcceptorObserver = Observer<SocketAcceptor, ReadableNotification>;
    using ErrorObserver = Observer<SocketAcceptor, ErrorNotification>;

    SocketAcceptor() = delete;
    SocketAcceptor(const SocketAcceptor &) = delete;
    SocketAcceptor & operator=(const SocketAcceptor &) = delete;

    explicit SocketAcceptor(
        const String & name_,
        Context & keeper_context_,
        ServerSocket & socket_,
        MainReactorPtr & main_reactor_,
        const Poco::Timespan & timeout_,
        size_t worker_count_ = getNumberOfPhysicalCPUCores(),
        SocketConfigurator socket_configurator_ = nullptr)
        : name(name_)
        , socket(socket_)
        , main_reactor(main_reactor_)
        , worker_count(worker_count_)
        , keeper_context(keeper_context_)
        , timeout(timeout_)
        , socket_configurator(socket_configurator_)
        , log(&Poco::Logger::get("SocketAcceptor"))
    {
        initialize();
    }

    virtual ~SocketAcceptor()
    {
        try
        {
            if (main_reactor)
            {
                main_reactor->removeEventHandler(socket, AcceptorObserver(*this, &SocketAcceptor::onAccept));
                main_reactor->removeEventHandler(socket, ErrorObserver(*this, &SocketAcceptor::onError));
            }
        }
        catch (...)
        {
        }
    }

    /// Accepts connection and dispatches event handler.
    void onAccept(const Notification &)
    {
        StreamSocket sock;
        try
        {
            LOG_TRACE(log, "{} tries to accept connection", socket.address().toString());
            sock = socket.acceptConnection();
            if (socket_configurator)
                socket_configurator(sock);
            LOG_TRACE(log, "Successfully accepted {}", sock.peerAddress().toString());
            createServiceHandler(sock);
        }
        catch (...)
        {
            LOG_ERROR(log, "Failed to accept a connection");
            sock.close();
        }
    }

    virtual void onError(const Notification & enf)
    {
        auto error_no = dynamic_cast<const ErrorNotification *>(&enf)->getErrorNo();
        throwFromErrno("Error when accepting connection for " + socket.address().toString(), ErrorCodes::EPOLL_ERROR, error_no);
    }

    /// Returns a reference to the listening socket.
    [[maybe_unused]] Socket & getSocket() { return socket; }

protected:
    void initialize()
    {
        /// Initialize worker getWorkerReactors
        poco_assert(worker_count > 0);
        for (size_t i = 0; i < worker_count; ++i)
            worker_reactors.push_back(std::make_shared<WorkerReactor>(timeout, name + "#" + std::to_string(i)));

        /// Register accept event handler to main reactor
        main_reactor->addEventHandler(socket, AcceptorObserver(*this, &SocketAcceptor::onAccept));
        main_reactor->addEventHandler(socket, ErrorObserver(*this, &SocketAcceptor::onError));

        /// It is necessary to wake up the getWorkerReactor.
        main_reactor->wakeUp();
    }

    /// Socket will be dispatched by socket_fd % worker_count.
    WorkerReactorPtr getWorkerReactor(const StreamSocket & socket_)
    {
        auto fd = socket_.impl()->sockfd();
        return worker_reactors[fd % worker_count];
    }

    /// Create and initialize a new ServiceHandler instance.
    virtual ServiceHandler * createServiceHandler(StreamSocket & socket_)
    {
        auto worker_reactor = getWorkerReactor(socket_);
        auto * handler = new ServiceHandler(keeper_context, socket_, *worker_reactor);

        /// It is necessary to wake up the getWorkerReactor.
        worker_reactor->wakeUp();
        return handler;
    }

private:
    /// Thread name prefix of the worker reactor threads.
    String name;

    /// Socket the main reactor bounded.
    ServerSocket socket;

    /// Main reactor which only concerns about the accept socket events of the socket.
    MainReactorPtr main_reactor;

    /// Number of workers which works in an independent thread.
    size_t worker_count;
    WorkerReactors worker_reactors;

    /// Keeper context
    Context & keeper_context;

    /// A period in which worker reactor poll waits.
    Poco::Timespan timeout;

    /// Used to configure accepted sockets
    SocketConfigurator socket_configurator;

    Poco::Logger * log;
};


}
