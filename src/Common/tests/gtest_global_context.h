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

#include <Interpreters/Context.h>

struct ContextHolder
{
    RK::SharedContextHolder shared_context;
    RK::Context context;

    ContextHolder()
        : shared_context(RK::Context::createShared())
        , context(RK::Context::createGlobal(shared_context.get()))
    {
        context.makeGlobalContext();
        context.setPath("./");
    }

    ContextHolder(ContextHolder &&) = default;
};

inline const ContextHolder & getContext()
{
    static ContextHolder holder;
    return holder;
}
