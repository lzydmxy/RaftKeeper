#pragma once

#include <common/types.h>
#include <libnuraft/nuraft.hxx>


namespace RK
{

using nuraft::buffer;
using nuraft::ptr;

/// Zstd wrappers used by the log store to (de)compress entry payloads.
/// Frames self-describe uncompressed size, so callers only pass the compressed bytes.
class ZstdLogCodec
{
public:
    /// Compress `input` bytes into a fresh buffer. Returns null on error.
    static ptr<buffer> compress(const char * input, size_t input_size);

    /// Decompress `input` bytes into a fresh buffer sized from the frame header.
    /// Throws on error (corrupted or non-zstd data).
    static ptr<buffer> decompress(const char * input, size_t input_size);
};

}
