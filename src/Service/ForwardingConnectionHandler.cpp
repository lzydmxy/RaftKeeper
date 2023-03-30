#include <Service/ForwardingConnection.h>
#include <Service/ForwardingConnectionHandler.h>
#include <Service/FourLetterCommand.h>
#include <Service/formatHex.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <ZooKeeper/ZooKeeperIO.h>
#include <Poco/Net/NetException.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int SYSTEM_ERROR;
}

using Poco::NObserver;


ForwardingConnectionHandler::ForwardingConnectionHandler(Context & global_context_, StreamSocket & socket_, SocketReactor & reactor_)
    : log(&Logger::get("ForwardingConnectionHandler"))
    , sock(socket_)
    , reactor(reactor_)
    , global_context(global_context_)
    , keeper_dispatcher(global_context.getDispatcher())
    , responses(std::make_unique<ThreadSafeResponseQueue>())
{
    LOG_DEBUG(log, "New connection from {}", sock.peerAddress().toString());

    reactor.addEventHandler(
        sock, NObserver<ForwardingConnectionHandler, ReadableNotification>(*this, &ForwardingConnectionHandler::onSocketReadable));
    reactor.addEventHandler(
        sock, NObserver<ForwardingConnectionHandler, ErrorNotification>(*this, &ForwardingConnectionHandler::onSocketError));
    reactor.addEventHandler(
        sock, NObserver<ForwardingConnectionHandler, ShutdownNotification>(*this, &ForwardingConnectionHandler::onReactorShutdown));
}

ForwardingConnectionHandler::~ForwardingConnectionHandler()
{
    try
    {
        reactor.removeEventHandler(
            sock, NObserver<ForwardingConnectionHandler, ReadableNotification>(*this, &ForwardingConnectionHandler::onSocketReadable));
        reactor.removeEventHandler(
            sock, NObserver<ForwardingConnectionHandler, WritableNotification>(*this, &ForwardingConnectionHandler::onSocketWritable));
        reactor.removeEventHandler(
            sock, NObserver<ForwardingConnectionHandler, ErrorNotification>(*this, &ForwardingConnectionHandler::onSocketError));
        reactor.removeEventHandler(
            sock, NObserver<ForwardingConnectionHandler, ShutdownNotification>(*this, &ForwardingConnectionHandler::onReactorShutdown));
    }
    catch (...)
    {
    }
}

void ForwardingConnectionHandler::onSocketReadable(const AutoPtr<ReadableNotification> & /*pNf*/)
{
    try
    {
        LOG_TRACE(log, "Forwarding handler socket readable");
        if (!sock.available())
        {
            LOG_INFO(log, "Client close connection!");
            destroyMe();
            return;
        }

        while (sock.available())
        {
            LOG_TRACE(log, "forwarding handler socket available");

            if (current_package.is_done)
            {
                LOG_TRACE(log, "Try handle new package");

                if (!req_header_buf.isFull())
                {
                    sock.receiveBytes(req_header_buf);
                    if (!req_header_buf.isFull())
                        continue;
                }

                /// read forward_protocol
                int8_t forward_type{};
                ReadBufferFromMemory read_buf(req_header_buf.begin(), req_header_buf.used());
                Coordination::read(forward_type, read_buf);
                req_header_buf.drain(req_header_buf.used());
                current_package.protocol = static_cast<ForwardType>(forward_type);

                LOG_TRACE(log, "Receive {}", toString(current_package.protocol));

                WriteBufferFromFiFoBuffer out;

                switch (forward_type)
                {
                    case ForwardType::Handshake:
                    case ForwardType::Sessions:
                    case ForwardType::GetSession:
                    case ForwardType::UpdateSession:
                    case ForwardType::Op:
                        current_package.is_done = false;
                        break;
                    default:
                        LOG_ERROR(log, "Got unexpected forward package type {}", forward_type);
                        destroyMe();
                        return;
                }
            }
            else
            {
                if (unlikely (current_package.protocol == ForwardType::Handshake))
                {
                    if (!req_body_buf)
                    {
                        /// server client
                        req_body_buf = std::make_shared<FIFOBuffer>(8);
                    }
                    sock.receiveBytes(*req_body_buf);
                    if (!req_body_buf->isFull())
                        continue;

                    ReadBufferFromMemory body(req_body_buf->begin(), req_body_buf->used());

                    Coordination::read(server_id, body);
                    Coordination::read(client_id, body);

                    /// register session response callback
                    auto response_callback = [this](ForwardResponsePtr response) { sendResponse(response); };

                    keeper_dispatcher->registerForward({server_id, client_id}, response_callback);

                    LOG_INFO(log, "Register forward from server {} client {}", server_id, client_id);


                    std::shared_ptr<ForwardHandshakeResponse> response = std::make_shared<ForwardHandshakeResponse>();
                    response->accepted = true;
                    response->error_code = nuraft::cmd_result_code::OK;

                    keeper_dispatcher->sendForwardResponse({server_id, client_id}, response);

                    req_body_buf.reset();
                    current_package.is_done = true;
                }
                else if (
                    current_package.protocol == ForwardType::Op || current_package.protocol == ForwardType::GetSession
                    || current_package.protocol == ForwardType::UpdateSession)
                {
                    auto request = ForwardRequestFactory::instance().get(current_package.protocol);
                    try
                    {
                        if (!req_body_buf) /// new data package
                        {
                            if (!req_body_len_buf.isFull())
                            {
                                sock.receiveBytes(req_body_len_buf);
                                if (!req_body_len_buf.isFull())
                                    continue;
                            }

                            /// request body length
                            int32_t body_len{};
                            ReadBufferFromMemory read_buf(req_body_len_buf.begin(), req_body_len_buf.used());

                            Coordination::read(body_len, read_buf);
                            req_body_len_buf.drain(req_body_len_buf.used());

                            LOG_TRACE(log, "Read request body length done, body length : {}", body_len);
                            req_body_buf = std::make_shared<FIFOBuffer>(body_len);
                        }

                        sock.receiveBytes(*req_body_buf);
                        if (!req_body_buf->isFull())
                            continue;

                        ReadBufferFromMemory body(req_body_buf->begin(), req_body_buf->used());

                        request->readImpl(body);

                        keeper_dispatcher->putForwardingRequest(server_id, client_id, request);

                        req_body_buf.reset();
                        current_package.is_done = true;
                    }
                    catch (Exception & e)
                    {
                        if (e.code() == ErrorCodes::SYSTEM_ERROR || e.code() == ErrorCodes::TIMEOUT_EXCEEDED)
                        {
                            auto response = request->makeResponse();
                            keeper_dispatcher->sendForwardResponse(server_id, client_id, response);
                            LOG_ERROR(log, "Error processing request {}", e.displayText());
                        }
                        else
                        {
                            tryLogCurrentException(log, __PRETTY_FUNCTION__);
                            throw e;
                        }
                    }
                    catch (Poco::Exception & e)
                    {
                        tryLogCurrentException(log, __PRETTY_FUNCTION__);
                        throw e;
                    }
                }
                else if (current_package.protocol == ForwardType::Sessions)
                {
                    auto request = ForwardRequestFactory::instance().get(current_package.protocol);
                    try
                    {
                        int32_t session_count{};
                        if (!req_body_buf) /// new data package
                        {
                            if (!req_body_len_buf.isFull())
                            {
                                sock.receiveBytes(req_body_len_buf);
                                if (!req_body_len_buf.isFull())
                                    continue;
                            }

                            /// request body length
                            ReadBufferFromMemory read_buf(req_body_len_buf.begin(), req_body_len_buf.used());

                            int32_t req_body_len{};
                            Coordination::read(req_body_len, read_buf);
                            req_body_len_buf.drain(req_body_len_buf.used());

                            session_count = req_body_len / 16;

                            LOG_TRACE(log, "Read request done, session count : {}", session_count);
                            req_body_buf = std::make_shared<FIFOBuffer>(req_body_len);
                        }

                        sock.receiveBytes(*req_body_buf);
                        if (!req_body_buf->isFull())
                            continue;

                        ReadBufferFromMemory body(req_body_buf->begin(), req_body_buf->used());
                        for (auto i = 0; i < session_count; ++i)
                        {
                            int64_t session_id;
                            Coordination::read(session_id, body);

                            int64_t expiration_time;
                            Coordination::read(expiration_time, body);

                            LOG_TRACE(log, "Receive remote session {}, expiration time {}", session_id, expiration_time);
                            keeper_dispatcher->handleRemoteSession(session_id, expiration_time);
                        }

                        req_body_buf.reset();
                        current_package.is_done = true;

                        auto response = request->makeResponse();

                        keeper_dispatcher->sendForwardResponse({server_id, client_id}, response);
                    }
                    catch (...)
                    {
                        auto response = request->makeResponse();
                        response->accepted = false;
                        keeper_dispatcher->sendForwardResponse({server_id, client_id}, response);
                        tryLogCurrentException(log, "Error processing ping request.");
                    }
                }
            }
        }
    }
    catch (Poco::Net::NetException &)
    {
        tryLogCurrentException(log, "Network error when receiving request, will close connection.");
        destroyMe();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error when handling request, will close connection.");
        destroyMe();
    }
}

void ForwardingConnectionHandler::onSocketWritable(const AutoPtr<WritableNotification> &)
{
    try
    {
        if (responses->empty() && send_buf.used() == 0)
            return;

        /// TODO use zero copy buffer
        size_t size_to_sent = 0;

        /// 1. accumulate data into tmp_buf
        responses->forEach([&size_to_sent, this](const auto & resp) -> bool
        {
            if (size_to_sent + resp->used() < SENT_BUFFER_SIZE)
            {
                /// add whole resp to send_buf
                send_buf.write(resp->begin(), resp->used());
                size_to_sent += resp->used();
            }
            else if (size_to_sent + resp->used() == SENT_BUFFER_SIZE)
            {
                /// add whole resp to send_buf
                send_buf.write(resp->begin(), resp->used());
                size_to_sent += resp->used();
            }
            else
            {
                /// add part of resp to send_buf
                send_buf.write(resp->begin(), SENT_BUFFER_SIZE - size_to_sent);
            }
            return size_to_sent < SENT_BUFFER_SIZE;
        });

        /// 2. send data
        size_t sent = sock.sendBytes(send_buf);

        /// 3. remove sent responses

        ptr<FIFOBuffer> resp;
        while (responses->peek(resp) && sent > 0)
        {
            if (sent >= resp->used())
            {
                sent -= resp->used();
                responses->remove();
            }
            else
            {
                resp->drain(sent);
                /// move data to begin
                resp->begin();
                sent = 0;
            }
        }

        /// If all sent unregister writable event.
        if (responses->empty() && send_buf.used() == 0)
        {
            LOG_TRACE(log, "Remove socket writable event handler - session {}", sock.peerAddress().toString());
            reactor.removeEventHandler(
                sock, NObserver<ForwardingConnectionHandler, WritableNotification>(*this, &ForwardingConnectionHandler::onSocketWritable));
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error when sending data to client, will close connection.");
        destroyMe();
    }
}

void ForwardingConnectionHandler::onReactorShutdown(const AutoPtr<ShutdownNotification> & /*pNf*/)
{
    LOG_INFO(log, "Reactor shutdown!");
    destroyMe();
}

void ForwardingConnectionHandler::onSocketError(const AutoPtr<ErrorNotification> & /*pNf*/)
{
    destroyMe();
}


void ForwardingConnectionHandler::sendResponse(ForwardResponsePtr response)
{
    LOG_TRACE(log, "Send response {}", response->toString());
    WriteBufferFromFiFoBuffer buf;
    response->write(buf);

    /// TODO handle timeout
    responses->push(buf.getBuffer());

    /// Trigger socket writable event
    reactor.addEventHandler(
        sock, NObserver<ForwardingConnectionHandler, WritableNotification>(*this, &ForwardingConnectionHandler::onSocketWritable));
    /// We must wake up reactor to interrupt it's sleeping.
    LOG_TRACE(
        log,
        "Poll trigger wakeup-- poco thread name {}, actually thread name {}",
        Poco::Thread::current() ? Poco::Thread::current()->name() : "main",
        getThreadName());

    reactor.wakeUp();
}

void ForwardingConnectionHandler::destroyMe()
{
    keeper_dispatcher->unRegisterForward(server_id, client_id);
    delete this;
}

}
