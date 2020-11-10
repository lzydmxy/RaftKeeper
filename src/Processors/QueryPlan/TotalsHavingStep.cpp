#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>

namespace DB
{

static ITransformingStep::Traits getTraits(bool has_filter)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = !has_filter,
        }
    };
}

TotalsHavingStep::TotalsHavingStep(
    const DataStream & input_stream_,
    bool overflow_row_,
    const ActionsDAGPtr & actions_,
    const std::string & filter_column_,
    TotalsMode totals_mode_,
    double auto_include_threshold_,
    bool final_)
    : ITransformingStep(
            input_stream_,
            TotalsHavingTransform::transformHeader(
                    input_stream_.header,
                    (actions_ ? std::make_shared<ExpressionActions>(actions_) : nullptr),
                    final_),
            getTraits(!filter_column_.empty()))
    , overflow_row(overflow_row_)
    , actions(actions_)
    , filter_column_name(filter_column_)
    , totals_mode(totals_mode_)
    , auto_include_threshold(auto_include_threshold_)
    , final(final_)
{
}

void TotalsHavingStep::transformPipeline(QueryPipeline & pipeline)
{
    auto totals_having = std::make_shared<TotalsHavingTransform>(
            pipeline.getHeader(), overflow_row, (actions ? std::make_shared<ExpressionActions>(actions) : nullptr),
            filter_column_name, totals_mode, auto_include_threshold, final);

    pipeline.addTotalsHavingTransform(std::move(totals_having));
}

static String totalsModeToString(TotalsMode totals_mode, double auto_include_threshold)
{
    switch (totals_mode)
    {
        case TotalsMode::BEFORE_HAVING:
            return "before_having";
        case TotalsMode::AFTER_HAVING_INCLUSIVE:
            return "after_having_inclusive";
        case TotalsMode::AFTER_HAVING_EXCLUSIVE:
            return "after_having_exclusive";
        case TotalsMode::AFTER_HAVING_AUTO:
            return "after_having_auto threshold " + std::to_string(auto_include_threshold);
    }

    __builtin_unreachable();
}

void TotalsHavingStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Filter column: " << filter_column_name << '\n';
    settings.out << prefix << "Mode: " << totalsModeToString(totals_mode, auto_include_threshold) << '\n';

    bool first = true;
    auto expression = std::make_shared<ExpressionActions>(actions);
    for (const auto & action : expression->getActions())
    {
        settings.out << prefix << (first ? "Actions: "
                                         : "         ");
        first = false;
        settings.out << action.toString() << '\n';
    }
}

}
