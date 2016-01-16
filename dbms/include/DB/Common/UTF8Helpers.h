#pragma once

#include <DB/Core/Types.h>

#if defined(__x86_64__)
	#include <x86intrin.h>
#else
	#include <DB/Common/ARMHelpers.h>
#endif


namespace DB
{


namespace UTF8
{


static const UInt8 CONTINUATION_OCTET_MASK = 0b11000000u;
static const UInt8 CONTINUATION_OCTET = 0b10000000u;

/// return true if `octet` binary repr starts with 10 (octet is a UTF-8 sequence continuation)
inline bool isContinuationOctet(const UInt8 octet)
{
	return (octet & CONTINUATION_OCTET_MASK) == CONTINUATION_OCTET;
}

/// moves `s` backward until either first non-continuation octet
inline void syncBackward(const UInt8 * & s)
{
	while (isContinuationOctet(*s))
		--s;
}

/// moves `s` forward until either first non-continuation octet or string end is met
inline void syncForward(const UInt8 * & s, const UInt8 * const end)
{
	while (s < end && isContinuationOctet(*s))
		++s;
}

/// returns UTF-8 code point sequence length judging by it's first octet
inline std::size_t seqLength(const UInt8 first_octet)
{
	if (first_octet < 0x80u)
		return 1;

	const std::size_t bits = 8;
	const auto first_zero = _bit_scan_reverse(static_cast<UInt8>(~first_octet));

	return bits - 1 - first_zero;
}


}


}
