#pragma once

#include <map>
#include <string>

#include <Common/IO/WriteBufferFromFile.h>
#include <libnuraft/nuraft.hxx>

#include <Service/KeeperStore.h>
#include <Service/KeeperUtils.h>
#include <Service/LogEntry.h>
#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wsuggest-destructor-override"
#    pragma clang diagnostic ignored "-Wheader-hygiene"
#endif
#ifdef __clang__
#    pragma clang diagnostic pop
#endif
#include <ZooKeeper/IKeeper.h>


namespace RK
{

const String MAGIC_SNAPSHOT_TAIL = "SnapTail";
const String MAGIC_SNAPSHOT_HEAD = "SnapHead";

/// 0.3KB / Node * 100M Count =  300MB
static constexpr UInt32 MAX_OBJECT_NODE_SIZE = 1000000;
/// 100M Count / 10K = 10K
static constexpr UInt32 SAVE_BATCH_SIZE = 10000;

using StringMap = std::unordered_map<String, String>;
using IntMap = std::unordered_map<String, int64_t>;
using BucketEdges = KeeperStore::BucketEdges;
using BucketNodes = KeeperStore::BucketNodes;

enum SnapshotVersion : uint8_t
{
    V0 = 0,
    V1 = 1, /// Add ACL map
    V2 = 2, /// Replace protobuf
    V3 = 3, /// Add last_log_term to file name
    None = 255,
};

String toString(SnapshotVersion version);


static constexpr auto CURRENT_SNAPSHOT_VERSION = SnapshotVersion::V2;

/// Batch data header in an snapshot object file.
struct SnapshotBatchHeader
{
    /// The length of the batch data (uncompressed)
    UInt32 data_length;
    /// The CRC32C of the batch data.
    /// If compression is enabled, this is the checksum of the compressed data.
    UInt32 data_crc;
    void reset()
    {
        data_length = 0;
        data_crc = 0;
    }
    static const size_t HEADER_SIZE = 8;
};

/// Batch data header in an snapshot object file.
enum class SnapshotBatchType : int
{
    SNAPSHOT_TYPE_DATA = 0,
    SNAPSHOT_TYPE_DATA_EPHEMERAL = 1,
    SNAPSHOT_TYPE_CONFIG = 2,
    SNAPSHOT_TYPE_SERVER = 3,
    SNAPSHOT_TYPE_SESSION = 4,
    SNAPSHOT_TYPE_STRINGMAP = 5,
    SNAPSHOT_TYPE_UINTMAP = 6,
    SNAPSHOT_TYPE_ACLMAP = 7
};

struct SnapshotBatchBody
{
    SnapshotBatchType type;
    std::vector<String> elements;

    void add(const String & element);
    size_t size() const;
    String & operator[](size_t n);

    static String serialize(const SnapshotBatchBody & batch_body);
    static ptr<SnapshotBatchBody> parse(const String & data);
};

int openFileForWrite(const String & path);
int openFileForRead(String & path);

/// snapshot object file header
bool isSnapshotFileHeader(UInt64 magic);

/// snapshot object file tail
bool isSnapshotFileTail(UInt64 magic);

ptr<WriteBufferFromFile> openFileAndWriteHeader(const String & path, SnapshotVersion version);
void writeTailAndClose(ptr<WriteBufferFromFile> & out, UInt32 checksum);

UInt32 updateCheckSum(UInt32 checksum, UInt32 data_crc);

/// Serialize and parse keeper node. Please note that children is ignored for we build parent relationship after load all data.
String serializeKeeperNode(const String & path, const ptr<KeeperNode> & node, SnapshotVersion version);
ptr<KeeperNodeWithPath> parseKeeperNode(const String & buf, SnapshotVersion version);


/// ----- For snapshot version 2 -----

/// save batch data in snapshot object
std::pair<size_t, UInt32> saveBatchV2(ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchBody> & batch);
std::pair<size_t, UInt32>
saveBatchAndUpdateCheckSumV2(ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchBody> & batch, UInt32 checksum);

void serializeAclsV2(ACLMap & acls, String path, UInt32 save_batch_size, SnapshotVersion version);
[[maybe_unused]] size_t
serializeEphemeralsV2(KeeperStore::Ephemerals & ephemerals, std::mutex & mutex, String path, UInt32 save_batch_size);

/// Serialize sessions and return the next_session_id before serialize
int64_t serializeSessionsV2(KeeperStore & store, UInt32 save_batch_size, SnapshotVersion version, String & path);

/// Save map<string, string> or map<string, uint64>
template <typename T>
void serializeMapV2(T & snap_map, UInt32 save_batch_size, SnapshotVersion version, String & path);

/// parse snapshot batch
void parseBatchDataV2(KeeperStore & store, SnapshotBatchBody & batch, BucketEdges & buckets_edges, BucketNodes & bucket_nodes, SnapshotVersion version);
void parseBatchSessionV2(KeeperStore & store, SnapshotBatchBody & batch, SnapshotVersion version);
void parseBatchAclMapV2(KeeperStore & store, SnapshotBatchBody & batch, SnapshotVersion version);
void parseBatchIntMapV2(KeeperStore & store, SnapshotBatchBody & batch, SnapshotVersion version);
}
