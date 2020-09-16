#ifndef __clang_analyzer__ // It's too hard to analyze.

#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{
struct SliceFromLeftConstantOffsetUnboundedSelectArraySource
    : public ArraySourceSelector<SliceFromLeftConstantOffsetUnboundedSelectArraySource>
{
    template <typename Source>
    static void selectImpl(Source && source, size_t & offset, ColumnArray::MutablePtr & result)
    {
        using Sink = typename std::remove_cv<Source>::type::SinkType;
        result = ColumnArray::create(source.createValuesColumn());
        Sink sink(result->getData(), result->getOffsets(), source.getColumnSize());
        sliceFromLeftConstantOffsetUnbounded(source, sink, offset);
    }
};

ColumnArray::MutablePtr sliceFromLeftConstantOffsetUnbounded(IArraySource & src, size_t offset)
{
    ColumnArray::MutablePtr res;
    SliceFromLeftConstantOffsetUnboundedSelectArraySource::select(src, offset, res);
    return res;
}
}

#endif
