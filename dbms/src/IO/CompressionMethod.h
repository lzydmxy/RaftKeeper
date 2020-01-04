#pragma once

#include <string>
#include <memory>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/** These are "generally recognizable" compression methods for data import/export.
  * Do not mess with more efficient compression methods used by ClickHouse internally
  *  (they use non-standard framing, indexes, checksums...)
  */

enum class CompressionMethod
{
    /// DEFLATE compression with gzip header and CRC32 checksum.
    /// This option corresponds to files produced by gzip(1) or HTTP Content-Encoding: gzip.
    Gzip,
    /// DEFLATE compression with zlib header and Adler32 checksum.
    /// This option corresponds to HTTP Content-Encoding: deflate.
    Zlib,
    Brotli,
    None
};

/// How the compression method is named in HTTP.
std::string toContentEncodingName(CompressionMethod method);

/** Choose compression method from path and hint.
  * if hint is "auto" or empty string, then path is analyzed,
  *  otherwise path parameter is ignored and hint is used as compression method name.
  * path is arbitrary string that will be analyzed for file extension (gz, br...) that determines compression.
  */
CompressionMethod chooseCompressionMethod(const std::string & path, const std::string & hint);

std::unique_ptr<ReadBuffer> wrapReadBufferWithCompressionMethod(std::unique_ptr<ReadBuffer> nested, CompressionMethod method);
std::unique_ptr<WriteBuffer> wrapWriteBufferWithCompressionMethod(std::unique_ptr<WriteBuffer> nested, CompressionMethod method, int level);

}
