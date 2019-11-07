#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>

namespace DB
{

namespace
{
    constexpr auto DATA_FILE_NAME = "data";
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}


MergeTreeDataPartWriterCompact::MergeTreeDataPartWriterCompact(
    const String & part_path_,
    const MergeTreeData & storage_,
    const NamesAndTypesList & columns_list_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_, 
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const WriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
: IMergeTreeDataPartWriter(part_path_,
    storage_, columns_list_,
    indices_to_recalc_, marks_file_extension_,
    default_codec_, settings_, index_granularity_)
{
    stream = std::make_unique<ColumnStream>(
        DATA_FILE_NAME,
        part_path + DATA_FILE_NAME, DATA_FILE_EXTENSION,
        part_path + DATA_FILE_NAME, marks_file_extension,
        default_codec,
        settings.max_compress_block_size,
        settings.estimated_size,
        settings.aio_threshold);
}

void MergeTreeDataPartWriterCompact::write(
    const Block & block, const IColumn::Permutation * permutation,
    const Block & primary_key_block, const Block & skip_indexes_block)
{
    size_t total_rows = block.rows();  
    size_t from_mark = current_mark;
    size_t current_row = 0;

    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical merge)
    if (compute_granularity)
        fillIndexGranularity(block);

    ColumnsWithTypeAndName columns_to_write(columns_list.size());
    auto it = columns_list.begin();
    for (size_t i = 0; i < columns_list.size(); ++i, ++it)
    {
        if (permutation)
        {
            if (primary_key_block.has(it->name))
                columns_to_write[i] = primary_key_block.getByName(it->name);
            else if (skip_indexes_block.has(it->name))
                columns_to_write[i] = skip_indexes_block.getByName(it->name);
            else
            {
                columns_to_write[i] = block.getByName(it->name);
                columns_to_write[i].column = columns_to_write[i].column->permute(*permutation, 0);
            }
        }
        else
            columns_to_write[i] = block.getByName(it->name);
    }

    std::cerr << "(MergeTreeDataPartWriterCompact::write) total_rows: " << total_rows << "\n";

    while (current_row < total_rows)
    {
        std::cerr << "(MergeTreeDataPartWriterCompact::write) current_row: " << current_row << "\n";

        bool write_marks = true;
        // size_t rows_to_write = std::min(total_rows, index_granularity.getMarkRows(current_mark));
        size_t rows_to_write = total_rows;
        index_granularity.appendMark(total_rows);

        if (rows_to_write)
            data_written = true;

        // if (current_row == 0 && index_offset != 0)
        // {
        //     rows_to_write = index_offset;
        //     write_marks = false;
        // }
        // else
        // {
        //     rows_to_write = index_granularity.getMarkRows(current_mark);
        // }

        // std::cerr << "(MergeTreeDataPartWriterCompact::write) rows_to_write: " << rows_to_write << "\n";

        /// There could already be enough data to compress into the new block.
        if (stream->compressed.offset() >= settings.min_compress_block_size)
             stream->compressed.next();
        
        size_t next_row = 0;

        if (write_marks)
        {
            writeIntBinary(rows_to_write, stream->marks);
            for (size_t i = 0; i < columns_to_write.size(); ++i)
            {
                writeIntBinary(stream->plain_hashing.count(), stream->marks);
                writeIntBinary(stream->compressed.offset(), stream->marks);
                next_row = writeColumnSingleGranule(columns_to_write[i], current_row, rows_to_write);
            }
            ++from_mark;
        }
        else
        {
            for (size_t i = 0; i < columns_to_write.size(); ++i)
                next_row = writeColumnSingleGranule(columns_to_write[i], current_row, rows_to_write);
        }

        current_row = next_row;
    }

    next_mark = from_mark;
    next_index_offset = total_rows - current_row;
}

size_t MergeTreeDataPartWriterCompact::writeColumnSingleGranule(const ColumnWithTypeAndName & column, size_t from_row, size_t number_of_rows)
{
    std::cerr << "(writeColumnSingleGranule) writing column: " << column.name << "\n";
    std::cerr << "(writeColumnSingleGranule) from_row: " << from_row << "\n";
    std::cerr << "(writeColumnSingleGranule) number_of_rows: " << number_of_rows << "\n";

    IDataType::SerializeBinaryBulkStatePtr state;
    IDataType::SerializeBinaryBulkSettings serialize_settings;

    serialize_settings.getter = [this](IDataType::SubstreamPath) -> WriteBuffer * { return &stream->compressed; };
    serialize_settings.position_independent_encoding = false;
    serialize_settings.low_cardinality_max_dictionary_size = 0;

    column.type->serializeBinaryBulkStatePrefix(serialize_settings, state);
    column.type->serializeBinaryBulkWithMultipleStreams(*column.column, from_row, number_of_rows, serialize_settings, state);
    column.type->serializeBinaryBulkStateSuffix(serialize_settings, state);

    return from_row + number_of_rows;
}

void MergeTreeDataPartWriterCompact::finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool write_final_mark, bool sync)
{
    UNUSED(write_final_mark);

    if (with_final_mark && data_written)
    {
        writeIntBinary(0ULL, stream->marks);
        for (size_t i = 0; i < columns_list.size(); ++i)
        {
            writeIntBinary(stream->plain_hashing.count(), stream->marks);
            writeIntBinary(stream->compressed.offset(), stream->marks);
        }
        if (compute_granularity)
            index_granularity.appendMark(0);
    }

    stream->finalize();
    if (sync)
        stream->sync();
    stream->addToChecksums(checksums);
    stream.reset();
}

}
