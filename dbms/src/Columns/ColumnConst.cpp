#include <IO/WriteHelpers.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

ColumnConst::ColumnConst(const ColumnPtr & data_, size_t s_)
    : data(data_), s(s_)
{
    /// Squash Const of Const.
    while (const ColumnConst * const_data = typeid_cast<const ColumnConst *>(data.get()))
        data = const_data->getDataColumnPtr();

    if (data->size() != 1)
        throw Exception("Incorrect size of nested column in constructor of ColumnConst: " + toString(data->size()) + ", must be 1.",
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
}

ColumnPtr ColumnConst::convertToFullColumn() const
{
    return data->replicate(Offsets(1, s));
}

ColumnPtr ColumnConst::removeLowCardinality() const
{
    return ColumnConst::create(data->convertToFullColumnIfLowCardinality(), s);
}

ColumnPtr ColumnConst::filter(const Filter & filt, ssize_t /*result_size_hint*/) const
{
    if (s != filt.size())
        throw Exception("Size of filter (" + toString(filt.size()) + ") doesn't match size of column (" + toString(s) + ")",
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    return ColumnConst::create(data, countBytesInFilter(filt));
}

ColumnPtr ColumnConst::replicate(const Offsets & offsets) const
{
    if (s != offsets.size())
        throw Exception("Size of offsets (" + toString(offsets.size()) + ") doesn't match size of column (" + toString(s) + ")",
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    size_t replicated_size = 0 == s ? 0 : offsets.back();
    return ColumnConst::create(data, replicated_size);
}

ColumnPtr ColumnConst::permute(const Permutation & perm, size_t limit) const
{
    if (limit == 0)
        limit = s;
    else
        limit = std::min(s, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation (" + toString(perm.size()) + ") is less than required (" + toString(limit) + ")",
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    return ColumnConst::create(data, limit);
}

ColumnPtr ColumnConst::index(const IColumn & indexes, size_t limit) const
{
    if (limit == 0)
        limit = indexes.size();

    if (indexes.size() < limit)
        throw Exception("Size of indexes (" + toString(indexes.size()) + ") is less than required (" + toString(limit) + ")",
                        ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    return ColumnConst::create(data, limit);
}

MutableColumns ColumnConst::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    if (s != selector.size())
        throw Exception("Size of selector (" + toString(selector.size()) + ") doesn't match size of column (" + toString(s) + ")",
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::vector<size_t> counts = countColumnsSizeInSelector(num_columns, selector);

    MutableColumns res(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        res[i] = cloneResized(counts[i]);

    return res;
}

void ColumnConst::getPermutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/, Permutation & res) const
{
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;
}


ColumnPtr recursiveMaterializeConstants(const ColumnPtr & column)
{
    if (!column)
        return column;

    if (const auto * column_array = typeid_cast<const ColumnArray *>(column.get()))
    {
        auto & data = column_array->getDataPtr();
        auto data_no_lc = recursiveMaterializeConstants(data);
        if (data.get() == data_no_lc.get())
            return column;

        return ColumnArray::create(data_no_lc, column_array->getOffsetsPtr());
    }

    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
    {
        auto & nested = column_const->getDataColumnPtr();
        auto nested_no_lc = recursiveMaterializeConstants(nested);
        if (nested.get() == nested_no_lc.get())
            return column;

        return ColumnConst::create(nested_no_lc, column_const->size());
    }

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        auto columns = column_tuple->getColumns();
        for (auto & element : columns)
            element = recursiveMaterializeConstants(element);
        return ColumnTuple::create(columns);
    }

    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
        return column_const->convertToFullColumn();

    return column;
}

}
