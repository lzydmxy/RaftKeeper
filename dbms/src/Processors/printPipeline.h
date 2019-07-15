#pragma once

#include <Processors/IProcessor.h>
#include <IO/Operators.h>

namespace DB
{

/** Print pipeline in "dot" format for GraphViz.
  * You can render it with:
  *  dot -T png < pipeline.dot > pipeline.png
  */

template <typename Processors, typename Statuses>
void printPipeline(const Processors & processors, const Statuses & statuses, WriteBuffer & out)
{
    out << "digraph\n{\n";

    auto get_proc_id = [](const IProcessor & proc) -> UInt64
    {
        return reinterpret_cast<std::uintptr_t>(&proc);
    };

    auto statuses_iter = statuses.begin();

    /// Nodes // TODO quoting and escaping
    for (const auto & processor : processors)
    {
        out << "n" << get_proc_id(*processor) << "[label=\"" << processor->getName() << processor->getDescription();

        if (statuses_iter != statuses.end())
        {
            out << " (" << IProcessor::statusToName(*statuses_iter) << ")";
            ++statuses_iter;
        }

        out << "\"];\n";
    }

    /// Edges
    for (const auto & processor : processors)
    {
        for (const auto & port : processor->getOutputs())
        {
            const IProcessor & curr = *processor;
            const IProcessor & next = port.getInputPort().getProcessor();

            out << "n" << get_proc_id(curr) << " -> " << "n" << get_proc_id(next) << ";\n";
        }
    }
    out << "}\n";
}

template <typename Processors>
void printPipeline(const Processors & processors, WriteBuffer & out)
{
    printPipeline(processors, std::vector<IProcessor::Status>(), out);
}

}
