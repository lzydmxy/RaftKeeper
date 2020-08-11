#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/Transforms/ArrayJoinTransform.h>
#include <Processors/QueryPipeline.h>
#include <Interpreters/ArrayJoinAction.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

ArrayJoinStep::ArrayJoinStep(const DataStream & input_stream_, ArrayJoinActionPtr array_join_)
    : ITransformingStep(
        input_stream_,
        ArrayJoinTransform::transformHeader(input_stream_.header, array_join_),
        getTraits())
    , array_join(std::move(array_join_))
{
}

void ArrayJoinStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipeline::StreamType::Totals;
        return std::make_shared<ArrayJoinTransform>(header, array_join, on_totals);
    });
}

void ArrayJoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    bool first = true;

    settings.out << prefix << (array_join->is_left ? "LEFT " : "") << "ARRAY JOIN ";
    for (const auto & column : array_join->columns)
    {
        if (!first)
            settings.out << ", ";
        first = false;


        settings.out << column;
    }
    settings.out << '\n';
}

}
