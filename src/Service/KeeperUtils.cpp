#include <Poco/Base64Encoder.h>
#include <Poco/File.h>
#include <Poco/SHA1Engine.h>

#include <Common/IO/WriteHelpers.h>
#include <boost/algorithm/string/split.hpp>

#include <Service/KeeperUtils.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <ZooKeeper/ZooKeeperIO.h>

using namespace nuraft;

namespace RK
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

String checkAndGetSuperdigest(const String & user_and_digest)
{
    if (user_and_digest.empty())
        return "";

    std::vector<String> scheme_and_id;
    boost::split(scheme_and_id, user_and_digest, [](char c) { return c == ':'; });
    if (scheme_and_id.size() != 2 || scheme_and_id[0] != "super")
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER, "Incorrect superdigest in keeper_server config. Must be 'super:base64string'");

    return user_and_digest;
}

nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(int64_t session_id, int64_t time, const Coordination::ZooKeeperRequestPtr & request)
{
    RK::WriteBufferFromNuraftBuffer buf;
    RK::writeIntBinary(session_id, buf);
    request->write(buf);
    Coordination::write(time, buf);
    return buf.getBuffer();
}


ptr<log_entry> makeClone(const ptr<log_entry> & entry)
{
    ptr<log_entry> clone = cs_new<log_entry>(entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type());
    return clone;
}

String getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return String{&path[basename_start + 1], path.length() - basename_start - 1};
}


String getParentPath(const String & path)
{
    auto rslash_pos = path.rfind('/');
    if (rslash_pos > 0)
        return path.substr(0, rslash_pos);
    return "/";
}

String base64Encode(const String & decoded)
{
    std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ostr.exceptions(std::ios::failbit);
    Poco::Base64Encoder encoder(ostr);
    encoder.rdbuf()->setLineLength(0);
    encoder << decoded;
    encoder.close();
    return ostr.str();
}

String getSHA1(const String & userdata)
{
    Poco::SHA1Engine engine;
    engine.update(userdata);
    const auto & digest_id = engine.digest();
    return String{digest_id.begin(), digest_id.end()};
}

String generateDigest(const String & userdata)
{
    std::vector<String> user_password;
    boost::split(user_password, userdata, [](char c) { return c == ':'; });
    return user_password[0] + ":" + base64Encode(getSHA1(userdata));
}

}
