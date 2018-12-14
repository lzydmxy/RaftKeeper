#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct SqrtName { static constexpr auto name = "sqrt"; };
using FunctionSqrt = FunctionMathUnaryFloat64<UnaryFunctionVectorized<SqrtName, sqrt>>;

void registerFunctionSqrt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSqrt>(FunctionFactory::CaseInsensitive);
}

}
