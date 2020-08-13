#include <Core/MySQL/IMySQLReadPacket.h>
#include <sstream>
#include <Core/MySQL/PacketPayloadReadBuffer.h>
#include <IO/LimitReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
}

namespace MySQLProtocol
{

void IMySQLReadPacket::readPayload(ReadBuffer & in, uint8_t & sequence_id)
{
    PacketPayloadReadBuffer payload(in, sequence_id);
    payload.next();
    readPayloadImpl(payload);
    if (!payload.eof())
    {
        std::stringstream tmp;
        tmp << "Packet payload is not fully read. Stopped after " << payload.count() << " bytes, while " << payload.available() << " bytes are in buffer.";
        throw Exception(tmp.str(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
    }
}

void IMySQLReadPacket::readPayloadWithUnpacked(ReadBuffer & in)
{
    readPayloadImpl(in);
}

void LimitedReadPacket::readPayload(ReadBuffer &in, uint8_t &sequence_id)
{
    LimitReadBuffer limited(in, 10000, true, "too long MySQL packet.");
    IMySQLReadPacket::readPayload(limited, sequence_id);
}

void LimitedReadPacket::readPayloadWithUnpacked(ReadBuffer & in)
{
    LimitReadBuffer limited(in, 10000, true, "too long MySQL packet.");
    IMySQLReadPacket::readPayloadWithUnpacked(limited);
}

uint64_t readLengthEncodedNumber(ReadBuffer & buffer)
{
    char c{};
    uint64_t buf = 0;
    buffer.readStrict(c);
    auto cc = static_cast<uint8_t>(c);
    if (cc < 0xfc)
    {
        return cc;
    }
    else if (cc < 0xfd)
    {
        buffer.readStrict(reinterpret_cast<char *>(&buf), 2);
    }
    else if (cc < 0xfe)
    {
        buffer.readStrict(reinterpret_cast<char *>(&buf), 3);
    }
    else
    {
        buffer.readStrict(reinterpret_cast<char *>(&buf), 8);
    }
    return buf;
}

void readLengthEncodedString(String & s, ReadBuffer & buffer)
{
    uint64_t len = readLengthEncodedNumber(buffer);
    s.resize(len);
    buffer.readStrict(reinterpret_cast<char *>(s.data()), len);
}

}

}
