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

#include <iterator>

namespace ext
{
    /** \brief Returns collection of specified container-type.
     *    Retains stored value_type, constructs resulting collection using iterator range. */
    template <template <typename...> class ResultCollection, typename Collection>
    auto collection_cast(const Collection & collection)
    {
        using value_type = typename Collection::value_type;

        return ResultCollection<value_type>(std::begin(collection), std::end(collection));
    }

    /** \brief Returns collection of specified type.
     *    Performs implicit conversion of between source and result value_type, if available and required. */
    template <typename ResultCollection, typename Collection>
    auto collection_cast(const Collection & collection)
    {
        return ResultCollection(std::begin(collection), std::end(collection));
    }
}
