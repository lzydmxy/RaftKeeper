#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include "FunctionArrayMapped.h"
#include <Functions/FunctionFactory.h>

namespace DB
{
/// arrayCompact(['a', 'a', 'b', 'b', 'a']) = ['a', 'b', 'a'] - compact arrays
    namespace ErrorCodes
    {
        extern const int ILLEGAL_COLUMN;
    }

    struct ArrayCompactImpl
    {
        static bool useDefaultImplementationForConstants() { return true; }
        static bool needBoolean() { return false; }
        static bool needExpression() { return false; }
        static bool needOneArray() { return false; }

        static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
        {
            WhichDataType which(expression_return);

            if (which.isUInt8()) { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>()); }
            else if (which.isUInt16()) { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt16>()); }
            else if (which.isUInt32()) { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()); }
            else if (which.isUInt64()) { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()); }
            else if (which.isInt8()) { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>()); }
            else if (which.isInt16()) { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt16>()); }
            else if (which.isInt32()) { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()); }
            else if (which.isInt64()) { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>()); }
            else if (which.isFloat32()) { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()); }
            else if (which.isFloat64()) { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>()); }


            throw Exception("arrayCompact cannot add values of type " + expression_return->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        template <typename T>
        static bool executeType(const ColumnPtr & mapped, const ColumnArray & array, ColumnPtr & res_ptr)
        {
            const ColumnVector<T> * column = checkAndGetColumn<ColumnVector<T>>(&*mapped);

            if (!column)
                return false;

            const IColumn::Offsets & offsets = array.getOffsets();
            const typename ColumnVector<T>::Container & data = column->getData();
            auto column_data = ColumnVector<T>::create(data.size());
            typename ColumnVector<T>::Container & res_values = column_data->getData();
            auto column_offsets = ColumnArray::ColumnOffsets::create(offsets.size());
            IColumn::Offsets & res_offsets = column_offsets->getData();

            size_t res_pos = 0;
            size_t pos = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                if (pos < offsets[i])
                {
                    res_values[res_pos] = data[pos];
                    for (++pos, ++res_pos; pos < offsets[i]; ++pos)
                    {
                        if (data[pos] != data[pos - 1])
                        {
                            res_values[res_pos++] = data[pos];
                        }
                    }
                }
                res_offsets[i] = res_pos;
            }
            for (size_t i = 0; i < data.size() - res_pos; ++i)
            {
                res_values.pop_back();
            }
            res_ptr = ColumnArray::create(std::move(column_data), std::move(column_offsets));
            return true;
        }

        static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
        {
            ColumnPtr res;

            if (executeType< UInt8 >(mapped, array, res) ||
                executeType< UInt16>(mapped, array, res) ||
                executeType< UInt32>(mapped, array, res) ||
                executeType< UInt64>(mapped, array, res) ||
                executeType< Int8  >(mapped, array, res) ||
                executeType< Int16 >(mapped, array, res) ||
                executeType< Int32 >(mapped, array, res) ||
                executeType< Int64 >(mapped, array, res) ||
                executeType<Float32>(mapped, array, res) ||
                executeType<Float64>(mapped, array, res))
                return res;
            else
                throw Exception("Unexpected column for arrayCompact: " + mapped->getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

    };

    struct NameArrayCompact { static constexpr auto name = "arrayCompact"; };
    using FunctionArrayCompact = FunctionArrayMapped<ArrayCompactImpl, NameArrayCompact>;

    void registerFunctionArrayCompact(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionArrayCompact>();
    }

}
