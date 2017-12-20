#pragma once

#include <vector>

#include <Core/Field.h>
#include <Common/FieldVisitors.h>


namespace DB
{

/** Parameters of different functions quantiles*.
  * - list of levels of quantiles.
  * It is also necessary to calculate an array of indices of levels that go in ascending order.
  *
  * Example: quantiles(0.5, 0.99, 0.95)(x).
  * levels: 0.5, 0.99, 0.95
  * levels_permutation: 0, 2, 1
  */
template <typename T>    /// float or double
struct QuantileLevels
{
    using Levels = std::vector<T>;
    using Permutation = std::vector<size_t>;

    Levels levels;
    Permutation permutation;    /// Index of the i-th level in `levels`.

    size_t size() const { return levels.size(); }

    QuantileLevels(const Array & params)
    {
        if (params.empty())
        {
            /// If levels are not specified, default is 0.5 (median).
            levels.push_back(0.5);
            permutation.push_back(0);
            return;
        }

        size_t size = params.size();
        levels.resize(size);
        permutation.resize(size);

        for (size_t i = 0; i < size; ++i)
        {
            levels[i] = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[i]);
            permutation[i] = i;
        }

        std::sort(permutation.begin(), permutation.end(), [this] (size_t a, size_t b) { return levels[a] < levels[b]; });
    }
};


}
