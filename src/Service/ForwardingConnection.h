#pragma once

#include <Service/KeeperStore.h>
#include <Service/WriteBufferFromFiFoBuffer.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <ZooKeeper/ZooKeeperIO.h>
#include <libnuraft/async.hxx>
#include <Poco/Net/StreamSocket.h>
#include <Common/IO/ReadBufferFromPocoSocket.h>
#include <Common/IO/WriteBufferFromPocoSocket.h>
#include <common/logger_useful.h>
#include <Service/ForwardRequest.h>
#include <Service/ForwardResponse.h>

namespace RK
{

class ForwardingConnection
{
public:
    ForwardingConnection(int32_t server_id_, int32_t thread_id_, String endpoint_, Poco::Timespan socket_timeout_)
        : my_server_id(server_id_)
        , thread_id(thread_id_)
        , endpoint(endpoint_)
        , socket_timeout(socket_timeout_)
        , log(&Poco::Logger::get("ForwardingConnection"))
    {
    }

    void connect();

    void send(ForwardRequestPtr request);
    bool receive(ForwardResponsePtr & response);

    void disconnect();

    /// Send hand shake to forwarding server,
    /// server will register me.
    void sendHandshake();

    [[maybe_unused]] [[maybe_unused]] bool receiveHandshake();

    void send(const KeeperStore::RequestForSession & request_for_session);
    bool receive(ForwardResponse & response);

    /// Send session to leader every session_sync_period_ms.
    void sendSession(const std::unordered_map<int64_t, int64_t> & session_to_expiration_time);

    bool poll(UInt64 timeout_microseconds);

    bool isConnected() const { return connected; }

    ~ForwardingConnection()
    {
        try
        {
            disconnect();
        }
        catch (...)
        {
            /// We must continue to execute all callbacks,
            /// because the user is waiting for them.
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    int32_t my_server_id;
    int32_t thread_id;

    std::atomic<bool> connected{false};
    String endpoint;

    /// socket send and receive timeout, but it not work for
    /// socket is non-blocking
    ///     For sending: loop to send n length @see WriteBufferFromPocoSocket.
    ///     For receiving: use poll to wait socket to be available.
    Poco::Timespan socket_timeout;
    Poco::Net::StreamSocket socket;

    /// socket read and write buffer
    std::optional<ReadBufferFromPocoSocket> in;
    std::optional<WriteBufferFromPocoSocket> out;

    Poco::Logger * log;
};
}
