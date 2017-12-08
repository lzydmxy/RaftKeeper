#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/WriteHelpers.h>
#include "FunctionsArithmetic.h"


namespace DB
{

const ColumnConst * checkAndGetColumnConstStringOrFixedString(const IColumn * column)
{
    if (!column->isConst())
        return {};

    const ColumnConst * res = static_cast<const ColumnConst *>(column);

    if (checkColumn<ColumnString>(&res->getDataColumn())
        || checkColumn<ColumnFixedString>(&res->getDataColumn()))
        return res;

    return {};
}


ColumnPtr convertConstTupleToTupleOfConstants(const ColumnConst & column)
{
    const ColumnTuple & src_tuple = static_cast<const ColumnTuple &>(column.getDataColumn());
    const Columns & src_tuple_columns = src_tuple.getColumns();
    size_t tuple_size = src_tuple_columns.size();
    size_t rows = column.size();

    Columns res_tuple_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        res_tuple_columns[i] = std::make_shared<ColumnConst>(src_tuple_columns[i], rows);

    return std::make_shared<ColumnTuple>(res_tuple_columns);
}


Block createBlockWithNestedColumns(const Block & block, ColumnNumbers args)
{
    std::sort(args.begin(), args.end());

    Block res;

    size_t j = 0;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & col = block.getByPosition(i);
        bool is_inserted = false;

        if ((j < args.size()) && (i == args[j]))
        {
            ++j;

            if (col.column->isNullable())
            {
                auto nullable_col = static_cast<const ColumnNullable *>(col.column.get());
                const ColumnPtr & nested_col = nullable_col->getNestedColumn();

                auto nullable_type = static_cast<const DataTypeNullable *>(col.type.get());
                const DataTypePtr & nested_type = nullable_type->getNestedType();

                res.insert(i, {nested_col, nested_type, col.name});

                is_inserted = true;
            }
        }

        if (!is_inserted)
            res.insert(i, col);
    }

    return res;
}


Block createBlockWithNestedColumns(const Block & block, ColumnNumbers args, size_t result)
{
    std::sort(args.begin(), args.end());

    Block res;

    size_t j = 0;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & col = block.getByPosition(i);
        bool is_inserted = false;

        if ((j < args.size()) && (i == args[j]))
        {
            ++j;

            if (col.type->isNullable())
            {
                bool is_const = col.column->isConst();
                auto const_col = typeid_cast<const ColumnConst *>(col.column.get());

                if (is_const && !const_col->getDataColumn().isNullable())
                    throw Exception("Column at position " + toString(i + 1) + " with type " + col.type->getName() +
                                    " should be nullable, but got " + const_col->getName(), ErrorCodes::LOGICAL_ERROR);

                auto nullable_col = static_cast<const ColumnNullable *>(
                        is_const ? &const_col->getDataColumn() : col.column.get());

                ColumnPtr nested_col = nullable_col->getNestedColumn();
                if (is_const)
                    nested_col = std::make_shared<ColumnConst>(nested_col, const_col->size());

                auto nullable_type = static_cast<const DataTypeNullable *>(col.type.get());
                const DataTypePtr & nested_type = nullable_type->getNestedType();

                res.insert(i, {nested_col, nested_type, col.name});

                is_inserted = true;
            }
        }
        else if (i == result)
        {
            if (col.type->isNullable())
            {
                auto nullable_type = static_cast<const DataTypeNullable *>(col.type.get());
                const DataTypePtr & nested_type = nullable_type->getNestedType();

                res.insert(i, {nullptr, nested_type, col.name});
                is_inserted = true;
            }
        }

        if (!is_inserted)
            res.insert(i, col);
    }

    return res;
}

}
