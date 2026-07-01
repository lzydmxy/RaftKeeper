#include <Service/ZstdLogCodec.h>
#include <Common/Exception.h>
#include <zstd.h>


namespace RK
{

namespace ErrorCodes
{
    extern const int CORRUPTED_LOG;
}

/// Level 3 is zstd's default: solid ratio and fast enough for the log write path.
static constexpr int ZSTD_LEVEL = 3;

ptr<buffer> ZstdLogCodec::compress(const char * input, size_t input_size)
{
    size_t bound = ZSTD_compressBound(input_size);
    ptr<buffer> out = buffer::alloc(bound);
    size_t written = ZSTD_compress(out->data_begin(), bound, input, input_size, ZSTD_LEVEL);
    if (ZSTD_isError(written))
        return nullptr;

    /// Shrink the buffer to the actual compressed size — allocate exactly and copy over.
    ptr<buffer> shrunk = buffer::alloc(written);
    memcpy(shrunk->data_begin(), out->data_begin(), written);
    shrunk->pos(0);
    return shrunk;
}

ptr<buffer> ZstdLogCodec::decompress(const char * input, size_t input_size)
{
    unsigned long long decompressed_size = ZSTD_getFrameContentSize(input, input_size);
    if (decompressed_size == ZSTD_CONTENTSIZE_ERROR)
        throw Exception(ErrorCodes::CORRUPTED_LOG, "Zstd log entry: invalid frame");
    if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN)
        throw Exception(ErrorCodes::CORRUPTED_LOG, "Zstd log entry: unknown decompressed size");

    ptr<buffer> out = buffer::alloc(static_cast<size_t>(decompressed_size));
    size_t actual = ZSTD_decompress(out->data_begin(), decompressed_size, input, input_size);
    if (ZSTD_isError(actual))
        throw Exception(ErrorCodes::CORRUPTED_LOG, "Zstd log entry: decompress failed: {}", ZSTD_getErrorName(actual));
    if (actual != decompressed_size)
        throw Exception(ErrorCodes::CORRUPTED_LOG, "Zstd log entry: short read {} vs {}", actual, decompressed_size);

    out->pos(0);
    return out;
}

}
