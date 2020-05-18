#pragma once
#include <DataStreams/IBlockInputStream.h>

namespace DB
{

class QueryPipeline;
class PullingPipelineExecutor;

/// Implement IBlockInputStream from QueryPipeline.
/// It's a temporary wrapper.
class PipelineExecutingBlockInputStream : public IBlockInputStream
{
public:
    explicit PipelineExecutingBlockInputStream(QueryPipeline pipeline_);
    ~PipelineExecutingBlockInputStream();

    String getName() const override { return "PipelineExecuting"; }
    Block getHeader() const override;

    void cancel(bool kill) override;

    /// Implement IBlockInputStream methods via QueryPipeline.
    void setProgressCallback(const ProgressCallback & callback) final;
    void setProcessListElement(QueryStatus * elem) final;
    void setLimits(const LocalLimits & limits_) final;
    void setQuota(const std::shared_ptr<const EnabledQuota> & quota_) final;
    void addTotalRowsApprox(size_t value) final;

protected:
    void readPrefixImpl() override;
    Block readImpl() override;

private:
    std::unique_ptr<PullingPipelineExecutor> executor;
    std::unique_ptr<QueryPipeline> pipeline;
};

}
