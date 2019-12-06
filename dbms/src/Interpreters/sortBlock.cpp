#include <Interpreters/sortBlock.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <Functions/FunctionHelpers.h>

#include <pdqsort.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_COLLATION;
}

static bool isCollationRequired(const SortColumnDescription & description)
{
    return description.collator != nullptr;
}


ColumnsWithSortDescriptions getColumnsWithSortDescription(const Block & block, const SortDescription & description)
{
    size_t size = description.size();
    ColumnsWithSortDescriptions res;
    res.reserve(size);

    for (size_t i = 0; i < size; ++i)
    {
        const IColumn * column = !description[i].column_name.empty()
            ? block.getByName(description[i].column_name).column.get()
            : block.safeGetByPosition(description[i].column_number).column.get();

        res.emplace_back(ColumnWithSortDescription{column, description[i], isColumnConst(*column)});
    }

    return res;
}


struct PartialSortingLess
{
    const ColumnsWithSortDescriptions & columns;

    explicit PartialSortingLess(const ColumnsWithSortDescriptions & columns_) : columns(columns_) {}

    bool operator() (size_t a, size_t b) const
    {
        for (ColumnsWithSortDescriptions::const_iterator it = columns.begin(); it != columns.end(); ++it)
        {
            int res;
            if (it->column_const)
                res = 0;
            else
                res = it->description.direction * it->column->compareAt(a, b, *it->column, it->description.nulls_direction);
            if (res < 0)
                return true;
            else if (res > 0)
                return false;
        }
        return false;
    }
};


struct PartialSortingLessWithCollation
{
    const ColumnsWithSortDescriptions & columns;

    explicit PartialSortingLessWithCollation(const ColumnsWithSortDescriptions & columns_)
        : columns(columns_)
    {
    }

    bool operator() (size_t a, size_t b) const
    {
        for (ColumnsWithSortDescriptions::const_iterator it = columns.begin(); it != columns.end(); ++it)
        {
            int res;

            if (it->column_const)
                res = 0;
            else if (isCollationRequired(it->description))
            {
                const ColumnString & column_string = assert_cast<const ColumnString &>(*it->column);
                res = column_string.compareAtWithCollation(a, b, *it->column, *it->description.collator);

            }
            else
                res = it->column->compareAt(a, b, *it->column, it->description.nulls_direction);
            res *= it->description.direction;
            if (res < 0)
                return true;
            else if (res > 0)
                return false;
        }
        return false;
    }
};

void sortBlock(Block & block, const SortDescription & description, UInt64 limit)
{
    if (!block)
        return;

    size_t size = block.rows();
    IColumn::Permutation perm(size);
    for (size_t i = 0; i < size; ++i)
        perm[i] = i;

    if (limit >= size)
        limit = 0;

    /// If only one column to sort by
    if (description.size() == 1)
    {
        bool reverse = description[0].direction == -1;

        const IColumn * column = !description[0].column_name.empty()
            ? block.getByName(description[0].column_name).column.get()
            : block.safeGetByPosition(description[0].column_number).column.get();

        if (isCollationRequired(description[0]))
        {
            /// it it's real string column, than we need sort
            if (const ColumnString * column_string = checkAndGetColumn<ColumnString>(column))
                column_string->getPermutationWithCollation(*description[0].collator, reverse, limit, perm);
            else if (!checkAndGetColumnConstData<ColumnString>(column))
                throw Exception("Collations could be specified only for String columns.", ErrorCodes::BAD_COLLATION);

        }
        else if (!isColumnConst(*column))
            column->getPermutation(reverse, limit, description[0].nulls_direction, perm);
        /// we don't need to do anything with const column

        size_t columns = block.columns();
        for (size_t i = 0; i < columns; ++i)
            block.getByPosition(i).column = block.getByPosition(i).column->permute(perm, limit);
    }
    else
    {
        bool need_collation = false;
        ColumnsWithSortDescriptions columns_with_sort_desc = getColumnsWithSortDescription(block, description);

        for (size_t i = 0, num_sort_columns = description.size(); i < num_sort_columns; ++i)
        {
            const IColumn * column = columns_with_sort_desc[i].column;
            if (isCollationRequired(description[i]))
            {
                if (!checkAndGetColumn<ColumnString>(column) && !checkAndGetColumnConstData<ColumnString>(column))
                    throw Exception("Collations could be specified only for String columns.", ErrorCodes::BAD_COLLATION);

                need_collation = true;
            }
        }

        if (need_collation)
        {
            PartialSortingLessWithCollation less_with_collation(columns_with_sort_desc);

            if (limit)
                std::partial_sort(perm.begin(), perm.begin() + limit, perm.end(), less_with_collation);
            else
                pdqsort(perm.begin(), perm.end(), less_with_collation);
        }
        else
        {
            PartialSortingLess less(columns_with_sort_desc);

            if (limit)
                std::partial_sort(perm.begin(), perm.begin() + limit, perm.end(), less);
            else
                pdqsort(perm.begin(), perm.end(), less);
        }

        size_t columns = block.columns();
        for (size_t i = 0; i < columns; ++i)
        {
            block.getByPosition(i).column = block.getByPosition(i).column->permute(perm, limit);
        }
    }
}


void stableGetPermutation(const Block & block, const SortDescription & description, IColumn::Permutation & out_permutation)
{
    if (!block)
        return;

    size_t size = block.rows();
    out_permutation.resize(size);
    for (size_t i = 0; i < size; ++i)
        out_permutation[i] = i;

    ColumnsWithSortDescriptions columns_with_sort_desc = getColumnsWithSortDescription(block, description);

    std::stable_sort(out_permutation.begin(), out_permutation.end(), PartialSortingLess(columns_with_sort_desc));
}


bool isAlreadySorted(const Block & block, const SortDescription & description)
{
    if (!block)
        return true;

    size_t rows = block.rows();

    ColumnsWithSortDescriptions columns_with_sort_desc = getColumnsWithSortDescription(block, description);

    PartialSortingLess less(columns_with_sort_desc);

    /** If the rows are not too few, then let's make a quick attempt to verify that the block is not sorted.
     * Constants - at random.
     */
    static constexpr size_t num_rows_to_try = 10;
    if (rows > num_rows_to_try * 5)
    {
        for (size_t i = 1; i < num_rows_to_try; ++i)
        {
            size_t prev_position = rows * (i - 1) / num_rows_to_try;
            size_t curr_position = rows * i / num_rows_to_try;

            if (less(curr_position, prev_position))
                return false;
        }
    }

    for (size_t i = 1; i < rows; ++i)
        if (less(i, i - 1))
            return false;

    return true;
}


void stableSortBlock(Block & block, const SortDescription & description)
{
    if (!block)
        return;

    IColumn::Permutation perm;
    stableGetPermutation(block, description, perm);

    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
        block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->permute(perm, 0);
}

}
