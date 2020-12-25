#pragma once
#include <Common/PODArray.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/UTF8Helpers.h>

#include <algorithm>
#include <climits>
#include <cstring>
#include <memory>
#include <utility>

#ifdef __SSE4_2__
#    include <nmmintrin.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

// used by FunctionsStringSimilarity and FunctionsStringHash
// includes extracting ASCII ngram, UTF8 ngram, ASCII word and UTF8 word
struct ExtractStringImpl
{
    // read a ASCII word
    static ALWAYS_INLINE inline const UInt8 * readOneASCIIWord(const UInt8 *& pos, const UInt8 * end)
    {
        // jump separators
        while (pos < end && !isAlphaNumericASCII(*pos))
            ++pos;

        // word start from here
        const UInt8 * word_start = pos;
        while (pos < end && isAlphaNumericASCII(*pos))
            ++pos;

        return word_start;
    }

    // read one UTF8 word from pos to word
    static ALWAYS_INLINE inline const UInt8 * readOneUTF8Word(const UInt8 *& pos, const UInt8 * end)
    {
        // jump UTF8 separator
        while (pos < end && isUTF8Sep(*pos))
            ++pos;

        // UTF8 word's character number
        const UInt8 * word_start = pos;

        while (pos < end && !isUTF8Sep(*pos))
            readOneUTF8Code(pos, end);

        return word_start;
    }

    // we use ASCII non-alphanum character as UTF8 separator
    static ALWAYS_INLINE inline bool isUTF8Sep(const UInt8 c) { return c < 128 && !isAlphaNumericASCII(c); }

    // read one UTF8 character
    static ALWAYS_INLINE inline void readOneUTF8Code(const UInt8 *& pos, const UInt8 * end)
    {
        size_t length = UTF8::seqLength(*pos);

        if (pos + length > end)
            length = end - pos;

        pos += length;
    }
};
}
