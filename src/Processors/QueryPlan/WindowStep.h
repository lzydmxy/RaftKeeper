#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

#include <Interpreters/AggregateDescription.h>

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

class WindowTransform;

class WindowStep : public ITransformingStep
{
public:
    using Transform = WindowTransform;

    explicit WindowStep(const DataStream & input_stream_,
            const WindowDescription & window_description_,
            const std::vector<WindowFunctionDescription> & window_functions_);

    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void describeActions(FormatSettings & settings) const override;

private:
    WindowDescription window_description;
    std::vector<WindowFunctionDescription> window_functions;
    Block input_header;
};

}
