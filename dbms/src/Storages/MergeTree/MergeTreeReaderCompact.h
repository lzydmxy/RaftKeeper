#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <port/clock.h>


namespace DB
{

/// Reader for compact parts
class MergeTreeReaderCompact : public IMergeTreeReader
{
public:
    MergeTreeReaderCompact(const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        const MarkRanges & mark_ranges_,
        const MergeTreeReaderSettings & settings_,
        const ValueSizeMap & avg_value_size_hints_ = ValueSizeMap{},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = ReadBufferFromFileBase::ProfileCallback{},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE);

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return false; }

private:
    bool isContinuousReading(size_t mark, size_t column_position);

    ReadBuffer * data_buffer;
    std::unique_ptr<CachedCompressedReadBuffer> cached_buffer;
    std::unique_ptr<CompressedReadBufferFromFile> non_cached_buffer;

    MergeTreeMarksLoader marks_loader;

    using ColumnPosition = std::optional<size_t>;
    /// Positions of columns in part structure.
    std::vector<ColumnPosition> column_positions;
    /// Should we read full column or only it's offsets
    std::vector<bool> read_only_offsets;

    size_t next_mark = 0;
    std::optional<std::pair<size_t, size_t>> last_read_granule;

    void initMarksLoader();
    void seekToMark(size_t row_index, size_t column_index);

    void readData(const String & name, IColumn & column, const IDataType & type,
        size_t from_mark, size_t column_position, size_t rows_to_read, bool only_offsets = false);

    ColumnPosition findColumnForOffsets(const String & column_name);
};

}
