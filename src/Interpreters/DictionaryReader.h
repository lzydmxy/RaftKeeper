#pragma once

#include <Core/Block.h>
#include <Columns/ColumnVector.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB
{

class Context;

/// Read block of required columns from Dictionary by UInt64 key column. Rename columns if needed.
/// Current implementation uses dictHas() + N * dictGet() functions.
class DictionaryReader
{
public:
    struct FunctionWrapper
    {
        ExecutableFunctionPtr function;
        ColumnNumbers arg_positions;
        size_t result_pos = 0;

        FunctionWrapper(FunctionOverloadResolverPtr resolver, const ColumnsWithTypeAndName & arguments, Block & block,
                        const ColumnNumbers & arg_positions_, const String & column_name, TypeIndex expected_type);

        void execute(Block & block, size_t rows) const
        {
            function->execute(block, arg_positions, result_pos, rows, false);
        }
    };

    DictionaryReader(const String & dictionary_name, const Names & src_column_names, const NamesAndTypesList & result_columns,
                     const Context & context);
    void readKeys(const IColumn & keys, Block & out_block, ColumnVector<UInt8>::Container & found, std::vector<size_t> & positions) const;

private:
    Block result_header;
    Block sample_block; /// dictionary name, column names, key, dictHas() result, dictGet() results
    size_t key_position;
    std::unique_ptr<FunctionWrapper> function_has;
    std::vector<FunctionWrapper> functions_get;

    static Block makeResultBlock(const NamesAndTypesList & names);
};

}
