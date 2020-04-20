#include <Core/MySQLClient.h>

namespace DB
{
using namespace MySQLProtocol;
using namespace MySQLProtocol::Authentication;

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

MySQLClient::MySQLClient(const String & host_, UInt16 port_, const String & user_, const String & password_, const String & database_)
    : host(host_), port(port_), user(user_), password(std::move(password_)), database(std::move(database_))
{
    client_capability_flags = CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH | CLIENT_SECURE_CONNECTION;
}

void MySQLClient::connect()
{
    try
    {
        if (connected)
        {
            close();
        }

        socket = std::make_unique<Poco::Net::StreamSocket>();
        address = DNSResolver::instance().resolveAddress(host, port);
        socket->connect(*address);

        in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
        out = std::make_shared<WriteBufferFromPocoSocket>(*socket);
        packet_sender = std::make_shared<PacketSender>(*in, *out, seq);
        connected = true;

        handshake();
    }
    catch (Poco::Net::NetException & e)
    {
        close();
        throw NetException(e.displayText(), ErrorCodes::NETWORK_ERROR);
    }
    catch (Poco::TimeoutException & e)
    {
        close();
        throw NetException(e.displayText(), ErrorCodes::SOCKET_TIMEOUT);
    }
}

void MySQLClient::close()
{
    in = nullptr;
    out = nullptr;
    if (socket)
        socket->close();
    socket = nullptr;
    connected = false;
}

/// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
void MySQLClient::handshake()
{
    Handshake handshake;
    packet_sender->receivePacket(handshake);
    if (handshake.auth_plugin_name != mysql_native_password)
    {
        throw Exception(
            "Only support " + mysql_native_password + " auth plugin name, but got " + handshake.auth_plugin_name,
            ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    }

    Native41 native41(password, handshake.auth_plugin_data);
    String auth_plugin_data = native41.getAuthPluginData();

    HandshakeResponse handshakeResponse(
        client_capability_flags, max_packet_size, charset_utf8, user, database, auth_plugin_data, mysql_native_password);
    packet_sender->sendPacket<HandshakeResponse>(handshakeResponse, true);

    PacketResponse packetResponse(handshake.capability_flags);
    packet_sender->receivePacket(packetResponse);

    switch (packetResponse.getType())
    {
        case PACKET_OK:
            break;
        case PACKET_ERR:
            throw Exception(packetResponse.err.error_message, ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
            break;
        case PACKET_EOF:
            break;
    }
}
}
