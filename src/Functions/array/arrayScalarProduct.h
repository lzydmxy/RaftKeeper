#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>


namespace DB
{

class Context;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}


template <typename Method, typename Name>
class FunctionArrayScalarProduct : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayScalarProduct>(); }

private:
    using ResultColumnType = ColumnVector<typename Method::ResultType>;

    template <typename T>
    bool executeNumber(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result) const
    {
        return executeNumberNumber<T, UInt8>(columns, arguments, result)
            || executeNumberNumber<T, UInt16>(columns, arguments, result)
            || executeNumberNumber<T, UInt32>(columns, arguments, result)
            || executeNumberNumber<T, UInt64>(columns, arguments, result)
            || executeNumberNumber<T, Int8>(columns, arguments, result)
            || executeNumberNumber<T, Int16>(columns, arguments, result)
            || executeNumberNumber<T, Int32>(columns, arguments, result)
            || executeNumberNumber<T, Int64>(columns, arguments, result)
            || executeNumberNumber<T, Float32>(columns, arguments, result)
            || executeNumberNumber<T, Float64>(columns, arguments, result);
    }


    template <typename T, typename U>
    bool executeNumberNumber(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result) const
    {
        ColumnPtr col1 = columns[arguments[0]].column->convertToFullColumnIfConst();
        ColumnPtr col2 = columns[arguments[1]].column->convertToFullColumnIfConst();
        if (!col1 || !col2)
            return false;

        const ColumnArray * col_array1 = checkAndGetColumn<ColumnArray>(col1.get());
        const ColumnArray * col_array2 = checkAndGetColumn<ColumnArray>(col2.get());
        if (!col_array1 || !col_array2)
            return false;

        if (!col_array1->hasEqualOffsets(*col_array2))
            throw Exception("Array arguments for function " + getName() + " must have equal sizes", ErrorCodes::BAD_ARGUMENTS);

        const ColumnVector<T> * col_nested1 = checkAndGetColumn<ColumnVector<T>>(col_array1->getData());
        const ColumnVector<U> * col_nested2 = checkAndGetColumn<ColumnVector<U>>(col_array2->getData());
        if (!col_nested1 || !col_nested2)
            return false;

        auto col_res = ResultColumnType::create();

        vector(
            col_nested1->getData(),
            col_nested2->getData(),
            col_array1->getOffsets(),
            col_res->getData());

        columns[result].column = std::move(col_res);
        return true;
    }

    template <typename T, typename U>
    static NO_INLINE void vector(
        const PaddedPODArray<T> & data1,
        const PaddedPODArray<U> & data2,
        const ColumnArray::Offsets & offsets,
        PaddedPODArray<typename Method::ResultType> & result)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            result[i] = Method::apply(&data1[current_offset], &data2[current_offset], array_size);
            current_offset = offsets[i];
        }
    }

public:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        // Basic type check
        std::vector<DataTypePtr> nested_types(2, nullptr);
        for (size_t i = 0; i < getNumberOfArguments(); ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (!array_type)
                throw Exception("All arguments for function " + getName() + " must be an array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto & nested_type = array_type->getNestedType();
            if (!isNativeNumber(nested_type) && !isEnum(nested_type))
                throw Exception(
                    getName() + " cannot process values of type " + nested_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            nested_types[i] = nested_type;
        }

        // Detail type check in Method, then return ReturnType
        return Method::getReturnType(nested_types[0], nested_types[1]);
    }

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t /* input_rows_count */) const override
    {
        if (!(executeNumber<UInt8>(columns, arguments, result)
            || executeNumber<UInt16>(columns, arguments, result)
              || executeNumber<UInt32>(columns, arguments, result)
              || executeNumber<UInt64>(columns, arguments, result)
              || executeNumber<Int8>(columns, arguments, result)
              || executeNumber<Int16>(columns, arguments, result)
              || executeNumber<Int32>(columns, arguments, result)
              || executeNumber<Int64>(columns, arguments, result)
              || executeNumber<Float32>(columns, arguments, result)
              || executeNumber<Float64>(columns, arguments, result)))
            throw Exception{"Illegal column " + columns[arguments[0]].column->getName() + " of first argument of function "
                                + getName(),
                            ErrorCodes::ILLEGAL_COLUMN};
    }
};

}

