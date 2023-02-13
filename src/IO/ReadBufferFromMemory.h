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

#include "SeekableReadBuffer.h"


namespace RK
{
/** Allows to read from memory range.
  * In comparison with just ReadBuffer, it only adds convenient constructors, that do const_cast.
  * In fact, ReadBuffer will not modify data in buffer, but it requires non-const pointer.
  */
class ReadBufferFromMemory : public SeekableReadBuffer
{
public:
    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    ReadBufferFromMemory(const CharT * buf, size_t size)
        : SeekableReadBuffer(const_cast<char *>(reinterpret_cast<const char *>(buf)), size, 0) {}

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;
};

}
