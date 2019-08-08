#pragma once

#include "config_formats.h"
#if USE_PARQUET
#    include <Processors/Formats/IOutputFormat.h>
#    include <Formats/FormatSettings.h>

namespace arrow
{
class Array;
class DataType;
}

namespace parquet
{
namespace arrow
{
    class FileWriter;
}
}

namespace DB
{
class ParquetBlockOutputFormat : public IOutputFormat
{
public:
    ParquetBlockOutputFormat(WriteBuffer & out_, const Block & header, const FormatSettings & format_settings);

    String getName() const override { return "ParquetBlockOutputFormat"; }
    void consume(Chunk) override;
    void finalize() override;

    String getContentType() const override { return "application/octet-stream"; }

private:
    const FormatSettings format_settings;

    std::unique_ptr<parquet::arrow::FileWriter> file_writer;
};

}

#endif
