#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

struct ArrayHasAnySelectArraySourcePair : public ArraySourcePairSelector<ArrayHasSelectArraySourcePair>
{
    template <typename FirstSource, typename SecondSource>
    static void selectSourcePair(FirstSource && first, SecondSource && second, ColumnUInt8 & result)
    {
        arrayAllAny<ArraySearchType::Any>(first, second, result);
    }
};


void sliceHasAny(IArraySource & first, IArraySource & second, ColumnUInt8 & result)
{
    ArrayHasAnySelectArraySourcePair::select(first, second, result);
}

}
