#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#include "IKeeper.h"
#include "ZooKeeperConstants.h"
#include <Common/IO/ReadBuffer.h>
#include <Common/IO/WriteBuffer.h>
#include <Common/IO/WriteHelpers.h>
#include <boost/noncopyable.hpp>
#include <Common/IO//Operators.h>
#include <Common/IO/ReadBufferFromString.h>


namespace Coordination
{

struct ZooKeeperResponse : virtual Response
{
    XID xid = 0;
    int64_t zxid;

    /// used to calculate request latency
    UInt64 request_created_time_ms = 0;

    virtual ~ZooKeeperResponse() override = default;
    virtual void readImpl(ReadBuffer &) = 0;
    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual void write(WriteBuffer & out) const;

    /// Prepended length to avoid copy
    virtual void writeNoCopy(WriteBufferFromOwnString & out) const;
    virtual OpNum getOpNum() const = 0;

    virtual bool operator==(const ZooKeeperResponse & response) const
    {
        if (const ZooKeeperResponse * zk_response = dynamic_cast<const ZooKeeperResponse *>(&response))
        {
            return error == zk_response->error && xid == zk_response->xid;
        }
        return false;
    }

    bool operator!=(const ZooKeeperResponse & response) const { return !(*this == response); }

    String toString() const override { return Response::toString() + ", xid " + std::to_string(xid) + ", zxid " + std::to_string(zxid); }
};

using ZooKeeperResponsePtr = std::shared_ptr<ZooKeeperResponse>;

struct ZooKeeperRequest : virtual Request
{
    XID xid = 0;
    bool has_watch = false;
    /// If the request was not send and the error happens, we definitely sure, that it has not been processed by the server.
    /// If the request was sent and we didn't get the response and the error happens, then we cannot be sure was it processed or not.
    bool probably_sent = false;

    bool restored_from_zookeeper_log = false;

    ZooKeeperRequest() = default;
    ZooKeeperRequest(const ZooKeeperRequest &) = default;
    virtual ~ZooKeeperRequest() override = default;

    virtual OpNum getOpNum() const = 0;

    /// Writes length, xid, op_num, then the rest.
    void write(WriteBuffer & out) const;

    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual void readImpl(ReadBuffer &) = 0;

    static std::shared_ptr<ZooKeeperRequest> read(ReadBuffer & in);

    virtual ZooKeeperResponsePtr makeResponse() const = 0;
    virtual bool isReadRequest() const = 0;
};

using ZooKeeperRequestPtr = std::shared_ptr<ZooKeeperRequest>;

struct ZooKeeperHeartbeatRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::Heartbeat; }
    void writeImpl(WriteBuffer &) const override { }
    void readImpl(ReadBuffer &) override { }
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
    String toString() const override { return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid); }
};

struct ZooKeeperHeartbeatResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override { }
    void writeImpl(WriteBuffer &) const override { }
    OpNum getOpNum() const override { return OpNum::Heartbeat; }
};

/** Internal request.
 */
struct ZooKeeperSetWatchesRequest final : ZooKeeperRequest
{
    int64_t relative_zxid;
    std::vector<String> data_watches;
    std::vector<String> exist_watches;
    std::vector<String> list_watches;

    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::SetWatches; }
    void writeImpl(WriteBuffer &) const override;
    void readImpl(ReadBuffer &) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
    String toString() const override { return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid); }
};

struct ZooKeeperSetWatchesResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override { }
    void writeImpl(WriteBuffer &) const override { }
    OpNum getOpNum() const override { return OpNum::SetWatches; }
};

struct ZooKeeperSyncRequest final : ZooKeeperRequest
{
    String path;
    String getPath() const override { return path; }
    OpNum getOpNum() const override { return OpNum::Sync; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override { return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path; }
};

struct ZooKeeperSyncResponse final : ZooKeeperResponse
{
    String path;
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Sync; }
};

/// Triggered watch response
struct ZooKeeperWatchResponse final : WatchResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;

    void write(WriteBuffer & out) const override;

    OpNum getOpNum() const override { return OpNum::Unspecified; }
};

struct ZooKeeperAuthRequest final : ZooKeeperRequest
{
    int32_t type = 0; /// ignored by the server
    String scheme;
    String data;

    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::Auth; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", type " + std::to_string(type) + ", scheme " + scheme
            + ", data" + data;
    }
};

struct ZooKeeperAuthResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override { }
    void writeImpl(WriteBuffer &) const override { }

    OpNum getOpNum() const override { return OpNum::Auth; }
};

struct ZooKeeperCloseRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::Close; }
    void writeImpl(WriteBuffer &) const override { }
    void readImpl(ReadBuffer &) override { }

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override { return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid); }
};

struct ZooKeeperCloseResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override { throw Exception("Received response for close request", Error::ZRUNTIMEINCONSISTENCY); }

    void writeImpl(WriteBuffer &) const override { }

    OpNum getOpNum() const override { return OpNum::Close; }
};

struct ZooKeeperCreateRequest final : public CreateRequest, ZooKeeperRequest
{
    /// used only during restore from zookeeper log
    int32_t parent_cversion = -1;

    ZooKeeperCreateRequest() = default;
    explicit ZooKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) { }

    OpNum getOpNum() const override { return OpNum::Create; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        //    String path;
        //    String data;
        //    bool is_ephemeral = false;
        //    bool is_sequential = false;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path + ", data " + data + ", is_ephemeral "
            + std::to_string(is_ephemeral) + ", is_sequential " + std::to_string(is_sequential);
    }
};

struct ZooKeeperCreateResponse final : CreateResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    OpNum getOpNum() const override { return OpNum::Create; }

    bool operator==(const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperCreateResponse * create_response = dynamic_cast<const ZooKeeperCreateResponse *>(&response))
        {
            return ZooKeeperResponse::operator==(response) && create_response->path_created == path_created;
        }
        return false;
    }

    String toString() const override { return "CreateResponse " + ZooKeeperResponse::toString() + ", path_created " + path_created; }
};

struct ZooKeeperRemoveRequest final : RemoveRequest, ZooKeeperRequest
{
    ZooKeeperRemoveRequest() = default;
    explicit ZooKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) { }

    OpNum getOpNum() const override { return OpNum::Remove; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        //    String path;
        //    int32_t version = -1;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path + ", version "
            + std::to_string(version);
    }
};

struct ZooKeeperRemoveResponse final : RemoveResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override { }
    void writeImpl(WriteBuffer &) const override { }
    OpNum getOpNum() const override { return OpNum::Remove; }
};

struct ZooKeeperExistsRequest final : ExistsRequest, ZooKeeperRequest
{
    ZooKeeperExistsRequest() = default;
    explicit ZooKeeperExistsRequest(const ExistsRequest & base) : ExistsRequest(base) { }
    OpNum getOpNum() const override { return OpNum::Exists; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
    String toString() const override
    {
        //    String path;
        //    int32_t version = -1;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path;
    }
};

struct ZooKeeperExistsResponse final : ExistsResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Exists; }

    bool operator==(const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperExistsResponse * exists_response = dynamic_cast<const ZooKeeperExistsResponse *>(&response))
        {
            return ZooKeeperResponse::operator==(response) && exists_response->stat == stat;
        }
        return false;
    }

    String toString() const override { return "ExistsResponse ," + ZooKeeperResponse::toString() + ", stat " + stat.toString(); }
};

struct ZooKeeperGetRequest final : GetRequest, ZooKeeperRequest
{
    ZooKeeperGetRequest() = default;
    explicit ZooKeeperGetRequest(const GetRequest & base) : GetRequest(base) { }
    OpNum getOpNum() const override { return OpNum::Get; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
    String toString() const override
    {
        //    String path;
        //    int32_t version = -1;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path;
    }
};

struct ZooKeeperGetResponse final : GetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Get; }

    bool operator==(const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperGetResponse * get_response = dynamic_cast<const ZooKeeperGetResponse *>(&response))
        {
            return ZooKeeperResponse::operator==(response) && get_response->stat == stat && get_response->data == data;
        }
        return false;
    }

    String toString() const override
    {
        return "GetResponse " + ZooKeeperResponse::toString() + ", stat " + stat.toString() + ", data " + data;
    }
};

struct ZooKeeperSetRequest final : SetRequest, ZooKeeperRequest
{
    ZooKeeperSetRequest() = default;
    explicit ZooKeeperSetRequest(const SetRequest & base) : SetRequest(base) { }

    OpNum getOpNum() const override { return OpNum::Set; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        //    String path;
        //    String data;
        //    int32_t version = -1;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path + "，data " + data + ", version "
            + std::to_string(version);
    }
};

struct ZooKeeperSetResponse final : SetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Set; }

    bool operator==(const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperSetResponse * set_response = dynamic_cast<const ZooKeeperSetResponse *>(&response))
        {
            return ZooKeeperResponse::operator==(response) && set_response->stat == stat;
        }
        return false;
    }

    String toString() const override { return "SetResponse " + ZooKeeperResponse::toString() + ", stat " + stat.toString(); }
};

struct ZooKeeperListRequest : ListRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::List; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
    String toString() const override { return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path; }
};

struct ZooKeeperSimpleListRequest final : ZooKeeperListRequest
{
    OpNum getOpNum() const override { return OpNum::SimpleList; }
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
    String toString() const override { return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path; }
};

struct ZooKeeperFilteredListRequest final : ZooKeeperListRequest
{
    enum class ListRequestType : UInt8
    {
        ALL,
        PERSISTENT_ONLY,
        EPHEMERAL_ONLY
    };

    String toString(ListRequestType value) const
    {
        static std::map<ListRequestType, String> type_to_name
            = {{ListRequestType::ALL, "ALL"},
               {ListRequestType::EPHEMERAL_ONLY, "PERSISTENT_ONLY"},
               {ListRequestType::PERSISTENT_ONLY, "EPHEMERAL_ONLY"}};

        if (auto it = type_to_name.find(value); it != type_to_name.end())
        {
            return it->second;
        }

        return "Unknown";
    }

    ListRequestType list_request_type{ListRequestType::ALL};

    OpNum getOpNum() const override { return OpNum::FilteredList; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    String toString() const override
    {
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path
            + toString(list_request_type);
    }
};

struct ZooKeeperListResponse final : ListResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::List; }

    bool operator==(const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperListResponse * list_response = dynamic_cast<const ZooKeeperListResponse *>(&response))
        {
            std::vector<String> copy_other_nodes = list_response->names.toStrings();
            std::vector<String> copy_nodes = names.toStrings();
            std::sort(copy_other_nodes.begin(), copy_other_nodes.end());
            std::sort(copy_nodes.begin(), copy_nodes.end());
            return ZooKeeperResponse::operator==(response) && list_response->stat == stat && copy_other_nodes == copy_nodes;
        }
        return false;
    }

    String toString() const override
    {
        String base = "ListResponse " + ZooKeeperResponse::toString() + ", stat " + stat.toString() + ", names ";
        auto func = [&](const StringRef & s) { base += ", " + s.toString(); };
        std::for_each(names.begin(), names.end(), func);
        return base;
    }
};

struct ZooKeeperSimpleListResponse final : SimpleListResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::SimpleList; }

    bool operator==(const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperSimpleListResponse * list_response = dynamic_cast<const ZooKeeperSimpleListResponse *>(&response))
        {
            std::vector<String> copy_other_nodes = list_response->names.toStrings();
            std::vector<String> copy_nodes = names.toStrings();
            std::sort(copy_other_nodes.begin(), copy_other_nodes.end());
            std::sort(copy_nodes.begin(), copy_nodes.end());
            return ZooKeeperResponse::operator==(response) && copy_other_nodes == copy_nodes;
        }
        return false;
    }

    String toString() const override
    {
        String base = "SimpleListResponse " + ZooKeeperResponse::toString() + ", names ";
        auto func = [&](const StringRef & s) { base += ", " + s.toString(); };
        std::for_each(names.begin(), names.end(), func);
        return base;
    }
};

struct ZooKeeperCheckRequest final : CheckRequest, ZooKeeperRequest
{
    ZooKeeperCheckRequest() = default;
    explicit ZooKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) { }

    OpNum getOpNum() const override { return OpNum::Check; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
    String toString() const override
    {
        //    String path;
        //    String data;
        //    int32_t version = -1;
        return Coordination::toString(getOpNum()) + ", xid " + std::to_string(xid) + ", path " + path + ", version "
            + std::to_string(version);
    }
};

struct ZooKeeperCheckResponse final : CheckResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override { }
    void writeImpl(WriteBuffer &) const override { }
    OpNum getOpNum() const override { return OpNum::Check; }
};

/// This response may be received only as an element of responses in MultiResponse.
struct ZooKeeperErrorResponse final : ErrorResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;

    OpNum getOpNum() const override { return OpNum::Error; }
};

struct ZooKeeperSetACLRequest final : SetACLRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::SetACL; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
};

struct ZooKeeperSetACLResponse final : SetACLResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::SetACL; }
};

struct ZooKeeperGetACLRequest final : GetACLRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::GetACL; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }
};

struct ZooKeeperGetACLResponse final : GetACLResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::GetACL; }
};

struct ZooKeeperMultiRequest final : MultiRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override;
    ZooKeeperMultiRequest() = default;

    ZooKeeperMultiRequest(const Requests & generic_requests, const ACLs & default_acls);

    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override;
    String toString() const override
    {
        String base = Coordination::toString(getOpNum());
        auto func = [&](const RequestPtr & value) { base += ", " + value->toString(); };
        std::for_each(requests.begin(), requests.end(), func);
        return base;
    }

    enum class OperationType : UInt8
    {
        Unspecified,
        Read,
        Write
    };

    OperationType operation_type = OperationType::Unspecified;

    void checkOperationType(OperationType type);
};

struct ZooKeeperMultiResponse : MultiResponse, ZooKeeperResponse
{
    OpNum getOpNum() const override { return OpNum::Multi; }

    explicit ZooKeeperMultiResponse(const Requests & requests)
    {
        responses.reserve(requests.size());

        for (const auto & request : requests)
            responses.emplace_back(dynamic_cast<const ZooKeeperRequest &>(*request).makeResponse());
    }

    explicit ZooKeeperMultiResponse(const Responses & responses_) { responses = responses_; }

    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    bool operator==(const ZooKeeperResponse & response) const override
    {
        if (const ZooKeeperMultiResponse * multi_response = dynamic_cast<const ZooKeeperMultiResponse *>(&response))
        {
            if (ZooKeeperResponse::operator==(response))
            {
                for (size_t i = 0; i < responses.size(); ++i)
                {
                    /// responses list must be ZooKeeperResponse ?
                    if (const ZooKeeperResponse * rhs_response
                        = dynamic_cast<const ZooKeeperResponse *>(multi_response->responses[i].get()))
                    {
                        if (const ZooKeeperResponse * lhs_response = dynamic_cast<const ZooKeeperResponse *>(responses[i].get()))
                            if (*rhs_response != *lhs_response)
                                return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    String toString() const override
    {
        String base = "MultiResponse " + ZooKeeperResponse::toString();
        auto func = [&](const ResponsePtr & value) { base += ", " + value->toString(); };
        std::for_each(responses.begin(), responses.end(), func);
        return base;
    }
};

struct ZooKeeperMultiWriteResponse final : public ZooKeeperMultiResponse
{
    using ZooKeeperMultiResponse::ZooKeeperMultiResponse;
    OpNum getOpNum() const override { return OpNum::Multi; }
};

struct ZooKeeperMultiReadResponse final : public ZooKeeperMultiResponse
{
    using ZooKeeperMultiResponse::ZooKeeperMultiResponse;
    OpNum getOpNum() const override { return OpNum::MultiRead; }
};

/// Fake internal RaftKeeper request. Never received from client
/// and never send to client. Used to create new session.
struct ZooKeeperNewSessionRequest final : ZooKeeperRequest
{
    /// The request processing framework is designed to be concurrent,
    /// with each request being assigned to a different thread based on session ID.
    /// For new session request the session there is no session id yet, so we use a fake id.
    int64_t internal_id;
    int32_t session_timeout_ms;
    /// Who requested this session
    int32_t server_id;

    Coordination::OpNum getOpNum() const override { return OpNum::NewSession; }
    String getPath() const override { return {}; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    Coordination::ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
};

/// Fake internal RaftKeeper response. Never received from client
/// and never send to client.
struct ZooKeeperNewSessionResponse final : ZooKeeperResponse
{
    /// internal id from request
    int64_t internal_id;
    int64_t session_id;
    /// Who requested this session
    int32_t server_id;
    bool success;

    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;

    Coordination::OpNum getOpNum() const override { return OpNum::NewSession; }

    String toString() const override
    {
        WriteBufferFromOwnString out;
        writeText(Coordination::toString(getOpNum()), out);
        writeText(", internal_id: " + std::to_string(internal_id), out);
        writeText(", session_id: " + std::to_string(session_id), out);
        writeText(", server_id: " + std::to_string(server_id), out);
        writeText(", success: " + std::to_string(success), out);
        writeText(", error: " + String(errorMessage(error)), out);
        return out.str();
    }
};


/// Fake internal RaftKeeper request. Never received from client
/// and never send to client. Used to session reconnect.
struct ZooKeeperUpdateSessionRequest final : ZooKeeperRequest
{
    int64_t session_id;
    int64_t session_timeout_ms;
    /// Who requested this session
    int32_t server_id;

    Coordination::OpNum getOpNum() const override { return OpNum::UpdateSession; }
    String getPath() const override { return {}; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    Coordination::ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
};

/// Fake internal RaftKeeper response. Never received from client
/// and never send to client.
struct ZooKeeperUpdateSessionResponse final : ZooKeeperResponse
{
    int64_t session_id;
    /// Who requested this session
    int32_t server_id;
    bool success;

    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;

    Coordination::OpNum getOpNum() const override { return OpNum::UpdateSession; }

    String toString() const override
    {
        WriteBufferFromOwnString out;
        writeText(Coordination::toString(getOpNum()), out);
        writeText(", session_id: " + std::to_string(session_id), out);
        writeText(", server_id: " + std::to_string(server_id), out);
        writeText(", success: " + std::to_string(success), out);
        writeText(", error: " + String(errorMessage(error)), out);
        return out.str();
    }
};

class ZooKeeperRequestFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<ZooKeeperRequestPtr()>;
    using OpNumToRequest = std::unordered_map<OpNum, Creator>;

    static ZooKeeperRequestFactory & instance();
    ZooKeeperRequestPtr get(OpNum op_num) const;
    void registerRequest(OpNum op_num, Creator creator);

private:
    OpNumToRequest op_num_to_request;
    ZooKeeperRequestFactory();
};

}
