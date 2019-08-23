#pragma once

#include <string>
#include <Columns/IColumn.h>
#include <Processors/Formats/IInputFormat.h>
#include <DataStreams/SizeLimits.h>
#include <Poco/Timespan.h>
#include <Common/Stopwatch.h>


namespace DB
{

/// Contains extra information about read data.
struct RowReadExtension
{
    /// IRowInputStream.read() output. It contains non zero for columns that actually read from the source and zero otherwise.
    /// It's used to attach defaults for partially filled rows.
    std::vector<UInt8> read_columns;
};

/// Common parameters for generating blocks.
struct RowInputFormatParams
{
    size_t max_block_size;

    UInt64 allow_errors_num;
    Float64 allow_errors_ratio;

    UInt64 rows_portion_size;

    using ReadCallback = std::function<void()>;
    ReadCallback callback;

    Poco::Timespan max_execution_time = 0;
    OverflowMode timeout_overflow_mode = OverflowMode::THROW;
};

///Row oriented input format: reads data row by row.
class IRowInputFormat : public IInputFormat
{
public:
    using Params = RowInputFormatParams;

    IRowInputFormat(
        Block header,
        ReadBuffer & in_,
        Params params_)
        : IInputFormat(std::move(header), in_), params(params_)
    {
    }

    Chunk generate() override;

protected:
    /** Read next row and append it to the columns.
      * If no more rows - return false.
      */
    virtual bool readRow(MutableColumns & columns, RowReadExtension & extra) = 0;

    virtual void readPrefix() {}                /// delimiter before begin of result
    virtual void readSuffix() {}                /// delimiter after end of result

    /// Skip data until next row.
    /// This is intended for text streams, that allow skipping of errors.
    /// By default - throws not implemented exception.
    virtual bool allowSyncAfterError() const { return false; }
    virtual void syncAfterError();

    /// In case of parse error, try to roll back and parse last one or two rows very carefully
    ///  and collect as much as possible diagnostic information about error.
    /// If not implemented, returns empty string.
    virtual std::string getDiagnosticInfo() { return {}; }

    const BlockMissingValues & getMissingValues() const override { return block_missing_values; }

private:
    Params params;
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

    size_t total_rows = 0;
    size_t num_errors = 0;

    BlockMissingValues block_missing_values;
};

}

