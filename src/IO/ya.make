LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/brotli/dec
    contrib/libs/brotli/enc
    contrib/libs/poco/NetSSL_OpenSSL
)

SRCS(
    AIO.cpp
    AIOContextPool.cpp
    BrotliReadBuffer.cpp
    BrotliWriteBuffer.cpp
    CascadeWriteBuffer.cpp
    CompressionMethod.cpp
    copyData.cpp
    createReadBufferFromFileBase.cpp
    createWriteBufferFromFileBase.cpp
    DoubleConverter.cpp
    HashingWriteBuffer.cpp
    HexWriteBuffer.cpp
    HTTPCommon.cpp
    LimitReadBuffer.cpp
    MemoryReadWriteBuffer.cpp
    MMapReadBufferFromFile.cpp
    MMapReadBufferFromFileDescriptor.cpp
    NullWriteBuffer.cpp
    parseDateTimeBestEffort.cpp
    PeekableReadBuffer.cpp
    Progress.cpp
    ReadBufferAIO.cpp
    ReadBufferFromFile.cpp
    ReadBufferFromFileBase.cpp
    ReadBufferFromFileDescriptor.cpp
    ReadBufferFromIStream.cpp
    ReadBufferFromMemory.cpp
    ReadBufferFromPocoSocket.cpp
    readFloatText.cpp
    ReadHelpers.cpp
    ReadWriteBufferFromHTTP.cpp
    SeekAvoidingReadBuffer.cpp
    UseSSL.cpp
    WriteBufferAIO.cpp
    WriteBufferFromFile.cpp
    WriteBufferFromFileBase.cpp
    WriteBufferFromFileDescriptor.cpp
    WriteBufferFromFileDescriptorDiscardOnFailure.cpp
    WriteBufferFromHTTP.cpp
    WriteBufferFromHTTPServerResponse.cpp
    WriteBufferFromOStream.cpp
    WriteBufferFromPocoSocket.cpp
    WriteBufferFromTemporaryFile.cpp
    WriteBufferValidUTF8.cpp
    WriteHelpers.cpp
    ZlibDeflatingWriteBuffer.cpp
    ZlibInflatingReadBuffer.cpp
)

END()
