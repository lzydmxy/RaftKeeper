#pragma once

#include <unordered_set>

#include <Poco/Delegate.h>
#include <Poco/FIFOBuffer.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Thread.h>
#include <Poco/Util/ServerApplication.h>

#include <Common/IO/ReadBufferFromString.h>
#include <Common/IO/WriteBufferFromString.h>
#include <Network/SocketAcceptor.h>
#include <Network/SocketNotification.h>
#include <Network/SocketReactor.h>

#include <Service/ConnCommon.h>
#include <Service/ConnectionStats.h>
#include <ZooKeeper/ZooKeeperCommon.h>


namespace RK
{
using Poco::Net::StreamSocket;

using Poco::FIFOBuffer;
using Poco::Logger;
using Poco::Thread;

/**
 * User connection handler with TCP protocol. It is a core class who process
 * Zookeeper network protocol and send it to dispatcher.
 *
 * We utilize reactor network programming model. We allocate a handler for
 * every connection and ensure that every handler run in the same network thread.
 *
 * So there is no multi-thread issues.
 */
class ConnectionHandler
{
public:
    static void registerConnection(ConnectionHandler * conn);
    static void unregisterConnection(ConnectionHandler * conn);

    /// dump all connections statistics, used for 4lw command
    static void dumpConnections(WriteBufferFromOwnString & buf, bool brief);
    /// reset statistics
    static void resetConnsStats();

private:
    static std::mutex conns_mutex;
    static std::unordered_set<ConnectionHandler *> connections;

public:
    ConnectionHandler(Context & global_context_, StreamSocket & socket_, SocketReactor & reactor_);
    ~ConnectionHandler();

    /// socket events: readable, writable, error
    void onSocketReadable(const Notification &);
    void onSocketWritable(const Notification &);

    void onReactorShutdown(const Notification &);
    void onSocketError(const Notification &);

    /// current connection statistics
    void dumpStats(WriteBufferFromOwnString & buf, bool brief);

    /// reset current connection statistics
    void resetStats();

private:
    Coordination::OpNum receiveHandshake(int32_t handshake_length);
    bool sendHandshake(const Coordination::ZooKeeperResponsePtr & response);
    static bool isHandShake(Int32 & handshake_length);

    void tryExecuteFourLetterWordCmd(int32_t four_letter_cmd);

    /// After handshake, we receive requests.
    std::pair<Coordination::OpNum, Coordination::XID> receiveRequest(int32_t length);
    /// Push a response of a user request to IO sending queue
    void pushUserResponseToSendingQueue(const Coordination::ZooKeeperResponsePtr & response);
    /// Push a response of new session or update session request to IO sending queue
    void sendSessionResponseToClient(const Coordination::ZooKeeperResponsePtr & response);

    /// do some statistics
    void packageSent();
    void packageReceived();

    /// do some statistics
    void updateStats(const Coordination::ZooKeeperResponsePtr & response);

    /// destroy connection
    void destroyMe();

    // Todo Add configuration sent_buffer_size
    static constexpr size_t SENT_BUFFER_SIZE = 16384;
    FIFOBuffer send_buf = FIFOBuffer(SENT_BUFFER_SIZE);

    /// Storing the result of the response serialization temporarily,
    /// We cannot directly serialize it onto send_buf，
    /// because `send_buf` maybe too small to hold a large size response.
    std::shared_ptr<ReadBufferFromOwnString> out_buffer;

    Logger * log;

    StreamSocket sock;
    String peer; /// remote peer address
    SocketReactor & reactor;

    FIFOBuffer req_header_buf = FIFOBuffer(4);

    /// request body length
    int32_t body_len{};
    std::shared_ptr<FIFOBuffer> req_body_buf;

    bool next_req_header_read_done = false;
    bool previous_req_body_read_done = true;

    /// Whether session established.
    std::atomic<bool> handshake_done = false;

    Context & global_context;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

    Poco::Timespan operation_timeout;
    Poco::Timespan session_timeout;
    Poco::Timespan min_session_timeout;
    Poco::Timespan max_session_timeout;

    /// Default session_id is 0, so if a connection failed,
    /// server will return 0 and when client tries connect
    /// with previous_session_id = 0.
    /// Server receives the 0 and will not identify it as a re-connection.
    std::atomic<int64_t> session_id{0};

    std::atomic<int64_t> internal_id{0};

    Stopwatch session_stopwatch;
    ThreadSafeResponseQueuePtr responses;

    /// connection established timestamp
    Poco::Timestamp established;
    LastOpMultiVersion last_op;

    ConnectionStats conn_stats;

    mutable std::mutex send_response_mutex;
    bool socket_writable_event_registered = false;
};

}
