#if !defined(ARCADIA_BUILD)

#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <string>

extern std::string_view errorCodeToName(int code);

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/** errorCodeToName() - returns the variable name for the error code.
  */
class FunctionErrorCodeToName : public IFunction
{
public:
    static constexpr auto name = "errorCodeToName";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionErrorCodeToName>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        if (!isNumber(types.at(0)))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of function {} must have simple numeric type, possibly Nullable", name);

        return std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, size_t input_rows_count) const override
    {
        auto & input_column = *arguments[0].column;
        auto col_res = res_type->createColumn();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const Int64 error_code = input_column.getInt(i);
            std::string_view error_name = errorCodeToName(error_code);
            col_res->insertData(error_name.data(), error_name.size());
        }

        return col_res;
    }
};


void registerFunctionErrorCodeToName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionErrorCodeToName>();
}

}

#endif
