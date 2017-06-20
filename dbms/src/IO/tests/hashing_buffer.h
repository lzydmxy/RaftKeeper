#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>

#define FAIL(msg) { std::cout << msg; exit(1); }


uint128 referenceHash(const char * data, size_t len)
{
    const size_t block_size = DBMS_DEFAULT_HASHING_BLOCK_SIZE;
    uint128 state(0, 0);
    size_t pos;

    for (pos = 0; pos + block_size <= len; pos += block_size)
    {
        state = DB::CityHash128WithSeed(data + pos, block_size, state);
    }

    if (pos < len)
        state = DB::CityHash128WithSeed(data + pos, len - pos, state);

    return state;
}
