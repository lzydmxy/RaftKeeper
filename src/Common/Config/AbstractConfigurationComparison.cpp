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
#include <Common/Config/AbstractConfigurationComparison.h>

#include <unordered_set>
#include <common/StringRef.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace RK
{
namespace
{
    String concatKeyAndSubKey(const String & key, const String & subkey)
    {
        // Copied from Poco::Util::ConfigurationView::translateKey():
        String result = key;
        if (!result.empty() && !subkey.empty() && subkey[0] != '[')
            result += '.';
        result += subkey;
        return result;
    };
}


bool isSameConfiguration(const Poco::Util::AbstractConfiguration & left, const Poco::Util::AbstractConfiguration & right)
{
    return isSameConfiguration(left, String(), right, String());
}


bool isSameConfiguration(const Poco::Util::AbstractConfiguration & left, const String & left_key,
                         const Poco::Util::AbstractConfiguration & right, const String & right_key)
{
    if (&left == &right && left_key == right_key)
        return true;

    bool has_property = left.hasProperty(left_key);
    if (has_property != right.hasProperty(right_key))
        return false;
    if (has_property)
    {
        /// The left and right configurations contains values so we can compare them.
        if (left.getRawString(left_key) != right.getRawString(right_key))
            return false;
    }

    /// Get the subkeys of the left and right configurations.
    Poco::Util::AbstractConfiguration::Keys subkeys;
    left.keys(left_key, subkeys);

    {
        /// Check that the right configuration has the same set of subkeys as the left configuration.
        Poco::Util::AbstractConfiguration::Keys right_subkeys;
        right.keys(right_key, right_subkeys);
        std::unordered_set<StringRef> left_subkeys{subkeys.begin(), subkeys.end()};
        if ((left_subkeys.size() != right_subkeys.size()) || (left_subkeys.size() != subkeys.size()))
            return false;
        for (const auto & right_subkey : right_subkeys)
            if (!left_subkeys.count(right_subkey))
                return false;
    }

    /// Go through all the subkeys and compare corresponding parts of the configurations.
    for (const auto & subkey : subkeys)
        if (!isSameConfiguration(left, concatKeyAndSubKey(left_key, subkey), right, concatKeyAndSubKey(right_key, subkey)))
            return false;

    return true;
}

}
