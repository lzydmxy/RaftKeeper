#include <Functions/IFunctionImpl.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int NOT_IMPLEMENTED;
}


/** Conversion to fixed string is implemented only for strings.
  */
class FunctionToFixedString : public IFunction
{
public:
    static constexpr auto name = "toFixedString";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToFixedString>(); }
    static FunctionPtr create() { return std::make_shared<FunctionToFixedString>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool isInjective(const Block &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isUnsignedInteger(arguments[1].type))
            throw Exception("Second argument for function " + getName() + " must be unsigned integer", ErrorCodes::ILLEGAL_COLUMN);
        if (!arguments[1].column)
            throw Exception("Second argument for function " + getName() + " must be constant", ErrorCodes::ILLEGAL_COLUMN);
        if (!isStringOrFixedString(arguments[0].type))
            throw Exception(getName() + " is only implemented for types String and FixedString", ErrorCodes::NOT_IMPLEMENTED);

        const size_t n = arguments[1].column->getUInt(0);
        return std::make_shared<DataTypeFixedString>(n);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const auto n = block.getByPosition(arguments[1]).column->getUInt(0);
        return executeForN(block, arguments, result, n);
    }

    static void executeForN(Block & block, const ColumnNumbers & arguments, const size_t result, const size_t n)
    {
        const auto & column = block.getByPosition(arguments[0]).column;

        if (const auto column_string = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto column_fixed = ColumnFixedString::create(n);

            auto & out_chars = column_fixed->getChars();
            const auto & in_chars = column_string->getChars();
            const auto & in_offsets = column_string->getOffsets();

            out_chars.resize_fill(in_offsets.size() * n);

            for (size_t i = 0; i < in_offsets.size(); ++i)
            {
                const size_t off = i ? in_offsets[i - 1] : 0;
                const size_t len = in_offsets[i] - off - 1;
                if (len > n)
                    throw Exception("String too long for type FixedString(" + toString(n) + ")",
                        ErrorCodes::TOO_LARGE_STRING_SIZE);
                memcpy(&out_chars[i * n], &in_chars[off], len);
            }

            block.getByPosition(result).column = std::move(column_fixed);
        }
        else if (const auto column_fixed_string = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            const auto src_n = column_fixed_string->getN();
            if (src_n > n)
                throw Exception{"String too long for type FixedString(" + toString(n) + ")", ErrorCodes::TOO_LARGE_STRING_SIZE};

            auto column_fixed = ColumnFixedString::create(n);

            auto & out_chars = column_fixed->getChars();
            const auto & in_chars = column_fixed_string->getChars();
            const auto size = column_fixed_string->size();
            out_chars.resize_fill(size * n);

            for (size_t i = 0; i < size; ++i)
                memcpy(&out_chars[i * n], &in_chars[i * src_n], src_n);

            block.getByPosition(result).column = std::move(column_fixed);
        }
        else
            throw Exception("Unexpected column: " + column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }
};

}

