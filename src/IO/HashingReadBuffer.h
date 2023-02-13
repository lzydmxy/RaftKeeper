/**
 * Copyright 2016-2023 ClickHouse, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace RK
{

/*
 * Calculates the hash from the read data. When reading, the data is read from the nested ReadBuffer.
 * Small pieces are copied into its own memory.
 */
class HashingReadBuffer : public IHashingBuffer<ReadBuffer>
{
public:
    explicit HashingReadBuffer(ReadBuffer & in_, size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : IHashingBuffer<ReadBuffer>(block_size_), in(in_)
    {
        working_buffer = in.buffer();
        pos = in.position();

        /// calculate hash from the data already read
        if (!working_buffer.empty())
        {
            calculateHash(pos, working_buffer.end() - pos);
        }
    }

private:
    bool nextImpl() override
    {
        in.position() = pos;
        bool res = in.next();
        working_buffer = in.buffer();
        pos = in.position();

        // `pos` may be different from working_buffer.begin() when using AIO.
        calculateHash(pos, working_buffer.end() - pos);

        return res;
    }

    ReadBuffer & in;
};

}
