#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include "IServer.h"
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/TestKeeperStorage.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <future>

namespace DB
{

class TestKeeperTCPHandler : public Poco::Net::TCPServerConnection
{
public:
    TestKeeperTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_)
        : Poco::Net::TCPServerConnection(socket_)
        , server(server_)
        , log(&Poco::Logger::get("TestKeeperTCPHandler"))
        , global_context(server.context())
        , test_keeper_storage(global_context.getTestKeeperStorage())
        , operation_timeout(Coordination::DEFAULT_OPERATION_TIMEOUT)
        , session_timeout(Coordination::DEFAULT_SESSION_TIMEOUT)
    {
    }

    void run() override;
private:
    IServer & server;
    Poco::Logger * log;
    Context global_context;
    std::shared_ptr<zkutil::TestKeeperStorage> test_keeper_storage;
    Poco::Timespan operation_timeout;
    Poco::Timespan session_timeout;

    std::queue<zkutil::TestKeeperStorage::AsyncResponse> responses;
    std::vector<zkutil::TestKeeperStorage::AsyncResponse> watch_responses;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;

    void runImpl();

    void sendHandshake();
    void receiveHandshake();

    bool receiveRequest();
};

}
