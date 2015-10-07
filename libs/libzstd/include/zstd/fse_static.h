/* ******************************************************************
   FSE : Finite State Entropy coder
   header file for static linking (only)
   Copyright (C) 2013-2015, Yann Collet

   BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

       * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   You can contact the author at :
   - Source repository : https://github.com/Cyan4973/FiniteStateEntropy
   - Public forum : https://groups.google.com/forum/#!forum/lz4c
****************************************************************** */
#pragma once

#if defined (__cplusplus)
extern "C" {
#endif


/******************************************
*  FSE API compatible with DLL
******************************************/
#include "fse.h"


/******************************************
*  Static allocation
******************************************/
/* FSE buffer bounds */
#define FSE_NCOUNTBOUND 512
#define FSE_BLOCKBOUND(size) (size + (size>>7))
#define FSE_COMPRESSBOUND(size) (FSE_NCOUNTBOUND + FSE_BLOCKBOUND(size))   /* Macro version, useful for static allocation */

/* You can statically allocate FSE CTable/DTable as a table of unsigned using below macro */
#define FSE_CTABLE_SIZE_U32(maxTableLog, maxSymbolValue)   (1 + (1<<(maxTableLog-1)) + ((maxSymbolValue+1)*2))
#define FSE_DTABLE_SIZE_U32(maxTableLog)                   (1 + (1<<maxTableLog))

/* Huff0 buffer bounds */
#define HUF_CTABLEBOUND 129
#define HUF_BLOCKBOUND(size) (size + (size>>8) + 8)   /* only true if pre-filtered with fast heuristic */
#define HUF_COMPRESSBOUND(size) (HUF_CTABLEBOUND + HUF_BLOCKBOUND(size))   /* Macro version, useful for static allocation */

/* You can statically allocate Huff0 DTable as a table of unsigned short using below macro */
#define HUF_DTABLE_SIZE_U16(maxTableLog)   (1 + (1<<maxTableLog))
#define HUF_CREATE_STATIC_DTABLE(DTable, maxTableLog) \
        unsigned short DTable[HUF_DTABLE_SIZE_U16(maxTableLog)] = { maxTableLog }


/******************************************
*  Error Management
******************************************/
#define FSE_LIST_ERRORS(ITEM) \
        ITEM(FSE_OK_NoError) ITEM(FSE_ERROR_GENERIC) \
        ITEM(FSE_ERROR_tableLog_tooLarge) ITEM(FSE_ERROR_maxSymbolValue_tooLarge) ITEM(FSE_ERROR_maxSymbolValue_tooSmall) \
        ITEM(FSE_ERROR_dstSize_tooSmall) ITEM(FSE_ERROR_srcSize_wrong)\
        ITEM(FSE_ERROR_corruptionDetected) \
        ITEM(FSE_ERROR_maxCode)

#define FSE_GENERATE_ENUM(ENUM) ENUM,
typedef enum { FSE_LIST_ERRORS(FSE_GENERATE_ENUM) } FSE_errorCodes;  /* enum is exposed, to detect & handle specific errors; compare function result to -enum value */


/******************************************
*  FSE advanced API
******************************************/
size_t FSE_countFast(unsigned* count, unsigned* maxSymbolValuePtr, const unsigned char* src, size_t srcSize);
/* same as FSE_count(), but blindly trust that all values within src are <= maxSymbolValuePtr[0] */

size_t FSE_buildCTable_raw (FSE_CTable* ct, unsigned nbBits);
/* build a fake FSE_CTable, designed to not compress an input, where each symbol uses nbBits */

size_t FSE_buildCTable_rle (FSE_CTable* ct, unsigned char symbolValue);
/* build a fake FSE_CTable, designed to compress always the same symbolValue */

size_t FSE_buildDTable_raw (FSE_DTable* dt, unsigned nbBits);
/* build a fake FSE_DTable, designed to read an uncompressed bitstream where each symbol uses nbBits */

size_t FSE_buildDTable_rle (FSE_DTable* dt, unsigned char symbolValue);
/* build a fake FSE_DTable, designed to always generate the same symbolValue */


/******************************************
*  FSE symbol compression API
******************************************/
/*
   This API consists of small unitary functions, which highly benefit from being inlined.
   You will want to enable link-time-optimization to ensure these functions are properly inlined in your binary.
   Visual seems to do it automatically.
   For gcc or clang, you'll need to add -flto flag at compilation and linking stages.
   If none of these solutions is applicable, include "fse.c" directly.
*/

typedef struct
{
    size_t bitContainer;
    int    bitPos;
    char*  startPtr;
    char*  ptr;
    char*  endPtr;
} FSE_CStream_t;

typedef struct
{
    ptrdiff_t   value;
    const void* stateTable;
    const void* symbolTT;
    unsigned    stateLog;
} FSE_CState_t;

size_t FSE_initCStream(FSE_CStream_t* bitC, void* dstBuffer, size_t maxDstSize);
void   FSE_initCState(FSE_CState_t* CStatePtr, const FSE_CTable* ct);

void   FSE_encodeSymbol(FSE_CStream_t* bitC, FSE_CState_t* CStatePtr, unsigned symbol);
void   FSE_addBits(FSE_CStream_t* bitC, size_t value, unsigned nbBits);
void   FSE_flushBits(FSE_CStream_t* bitC);

void   FSE_flushCState(FSE_CStream_t* bitC, const FSE_CState_t* CStatePtr);
size_t FSE_closeCStream(FSE_CStream_t* bitC);

/*
These functions are inner components of FSE_compress_usingCTable().
They allow the creation of custom streams, mixing multiple tables and bit sources.

A key property to keep in mind is that encoding and decoding are done **in reverse direction**.
So the first symbol you will encode is the last you will decode, like a LIFO stack.

You will need a few variables to track your CStream. They are :

FSE_CTable    ct;         // Provided by FSE_buildCTable()
FSE_CStream_t bitStream;  // bitStream tracking structure
FSE_CState_t  state;      // State tracking structure (can have several)


The first thing to do is to init bitStream and state.
    size_t errorCode = FSE_initCStream(&bitStream, dstBuffer, maxDstSize);
    FSE_initCState(&state, ct);

Note that FSE_initCStream() can produce an error code, so its result should be tested, using FSE_isError();
You can then encode your input data, byte after byte.
FSE_encodeSymbol() outputs a maximum of 'tableLog' bits at a time.
Remember decoding will be done in reverse direction.
    FSE_encodeByte(&bitStream, &state, symbol);

At any time, you can also add any bit sequence.
Note : maximum allowed nbBits is 25, for compatibility with 32-bits decoders
    FSE_addBits(&bitStream, bitField, nbBits);

The above methods don't commit data to memory, they just store it into local register, for speed.
Local register size is 64-bits on 64-bits systems, 32-bits on 32-bits systems (size_t).
Writing data to memory is a manual operation, performed by the flushBits function.
    FSE_flushBits(&bitStream);

Your last FSE encoding operation shall be to flush your last state value(s).
    FSE_flushState(&bitStream, &state);

Finally, you must close the bitStream.
The function returns the size of CStream in bytes.
If data couldn't fit into dstBuffer, it will return a 0 ( == not compressible)
If there is an error, it returns an errorCode (which can be tested using FSE_isError()).
    size_t size = FSE_closeCStream(&bitStream);
*/


/******************************************
*  FSE symbol decompression API
******************************************/
typedef struct
{
    size_t   bitContainer;
    unsigned bitsConsumed;
    const char* ptr;
    const char* start;
} FSE_DStream_t;

typedef struct
{
    size_t      state;
    const void* table;   /* precise table may vary, depending on U16 */
} FSE_DState_t;


size_t FSE_initDStream(FSE_DStream_t* bitD, const void* srcBuffer, size_t srcSize);
void   FSE_initDState(FSE_DState_t* DStatePtr, FSE_DStream_t* bitD, const FSE_DTable* dt);

unsigned char FSE_decodeSymbol(FSE_DState_t* DStatePtr, FSE_DStream_t* bitD);
size_t        FSE_readBits(FSE_DStream_t* bitD, unsigned nbBits);
unsigned int  FSE_reloadDStream(FSE_DStream_t* bitD);

unsigned FSE_endOfDStream(const FSE_DStream_t* bitD);
unsigned FSE_endOfDState(const FSE_DState_t* DStatePtr);

typedef enum { FSE_DStream_unfinished = 0,
               FSE_DStream_endOfBuffer = 1,
               FSE_DStream_completed = 2,
               FSE_DStream_tooFar = 3 } FSE_DStream_status;  /* result of FSE_reloadDStream() */
               /* 1,2,4,8 would be better for bitmap combinations, but slows down performance a bit ... ?! */

/*
Let's now decompose FSE_decompress_usingDTable() into its unitary components.
You will decode FSE-encoded symbols from the bitStream,
and also any other bitFields you put in, **in reverse order**.

You will need a few variables to track your bitStream. They are :

FSE_DStream_t DStream;    // Stream context
FSE_DState_t  DState;     // State context. Multiple ones are possible
FSE_DTable*   DTablePtr;  // Decoding table, provided by FSE_buildDTable()

The first thing to do is to init the bitStream.
    errorCode = FSE_initDStream(&DStream, srcBuffer, srcSize);

You should then retrieve your initial state(s)
(in reverse flushing order if you have several ones) :
    errorCode = FSE_initDState(&DState, &DStream, DTablePtr);

You can then decode your data, symbol after symbol.
For information the maximum number of bits read by FSE_decodeSymbol() is 'tableLog'.
Keep in mind that symbols are decoded in reverse order, like a LIFO stack (last in, first out).
    unsigned char symbol = FSE_decodeSymbol(&DState, &DStream);

You can retrieve any bitfield you eventually stored into the bitStream (in reverse order)
Note : maximum allowed nbBits is 25, for 32-bits compatibility
    size_t bitField = FSE_readBits(&DStream, nbBits);

All above operations only read from local register (which size depends on size_t).
Refueling the register from memory is manually performed by the reload method.
    endSignal = FSE_reloadDStream(&DStream);

FSE_reloadDStream() result tells if there is still some more data to read from DStream.
FSE_DStream_unfinished : there is still some data left into the DStream.
FSE_DStream_endOfBuffer : Dstream reached end of buffer. Its container may no longer be completely filled.
FSE_DStream_completed : Dstream reached its exact end, corresponding in general to decompression completed.
FSE_DStream_tooFar : Dstream went too far. Decompression result is corrupted.

When reaching end of buffer (FSE_DStream_endOfBuffer), progress slowly, notably if you decode multiple symbols per loop,
to properly detect the exact end of stream.
After each decoded symbol, check if DStream is fully consumed using this simple test :
    FSE_reloadDStream(&DStream) >= FSE_DStream_completed

When it's done, verify decompression is fully completed, by checking both DStream and the relevant states.
Checking if DStream has reached its end is performed by :
    FSE_endOfDStream(&DStream);
Check also the states. There might be some symbols left there, if some high probability ones (>50%) are possible.
    FSE_endOfDState(&DState);
*/


/******************************************
*  FSE unsafe symbol API
******************************************/
size_t FSE_readBitsFast(FSE_DStream_t* bitD, unsigned nbBits);
/* faster, but works only if nbBits >= 1 (otherwise, result will be corrupted) */

unsigned char FSE_decodeSymbolFast(FSE_DState_t* DStatePtr, FSE_DStream_t* bitD);
/* faster, but works only if allways nbBits >= 1 (otherwise, result will be corrupted) */


#if defined (__cplusplus)
}
#endif
