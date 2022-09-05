#pragma once

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Poco/Net/StreamSocket.h>
#include <common/logger_useful.h>
#include <Service/SvsKeeperStorage.h>
#include <Service/WriteBufferFromFiFoBuffer.h>

namespace DB
{

enum ForwardProtocol : int8_t
{
    Hello = 1,
    Ping = 2,
    Data = 3,
    Result = 4
};

struct ForwardResponse
{
    ForwardProtocol protocol;
    bool accepted;
    void write(WriteBufferFromFiFoBuffer & buf) const
    {
        Coordination::write(protocol, buf);
        Coordination::write(accepted, buf);
    }
};

class ForwardingConnection
{
public:
    ForwardingConnection(String endpoint_, Poco::Timespan operation_timeout_ms) : endpoint(endpoint_), operation_timeout(operation_timeout_ms), log(&Poco::Logger::get("ForwardingConnection")) {}

    void connect(Poco::Net::SocketAddress & address, Poco::Timespan connection_timeout);
    void send(SvsKeeperStorage::RequestForSession request_for_session);
    void disconnect();

    void sendHandshake();

    void receiveHandshake();

    void sendPing();

    void receivePing();

    ~ForwardingConnection()
    {
        try
        {
            disconnect();
        }
        catch (...)
        {
            /// We must continue to execute all callbacks, because the user is waiting for them.
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    bool connected{false};
    String endpoint;
    Poco::Timespan operation_timeout;
    Poco::Net::StreamSocket socket;
    std::optional<ReadBufferFromPocoSocket> in;
    std::optional<WriteBufferFromPocoSocket> out;

    Poco::Logger * log;

};
}
