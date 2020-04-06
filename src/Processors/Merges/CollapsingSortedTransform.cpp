#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitors.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

/// Maximum number of messages about incorrect data in the log.
#define MAX_ERROR_MESSAGES 10

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

CollapsingSortedTransform::CollapsingSortedTransform(
    const Block & header,
    size_t num_inputs,
    SortDescription description_,
    const String & sign_column,
    size_t max_block_size,
    WriteBuffer * out_row_sources_buf_,
    bool use_average_block_sizes)
    : IMergingTransform(num_inputs, header, header, true)
    , merged_data(header.cloneEmptyColumns(), use_average_block_sizes, max_block_size)
    , description(std::move(description_))
    , sign_column_number(header.getPositionByName(sign_column))
    , out_row_sources_buf(out_row_sources_buf_)
    , source_chunks(num_inputs)
    , cursors(num_inputs)
    , chunk_allocator(num_inputs + max_row_refs)
{
}

void CollapsingSortedTransform::initializeInputs()
{
    queue = SortingHeap<SortCursor>(cursors);
    is_queue_initialized = true;
}

void CollapsingSortedTransform::consume(Chunk chunk, size_t input_number)
{
    updateCursor(std::move(chunk), input_number);

    if (is_queue_initialized)
        queue.push(cursors[input_number]);
}

void CollapsingSortedTransform::updateCursor(Chunk chunk, size_t source_num)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);

    auto & source_chunk = source_chunks[source_num];

    if (source_chunk)
    {
        source_chunk = chunk_allocator.alloc(std::move(chunk));
        cursors[source_num].reset(source_chunk->getColumns(), {});
    }
    else
    {
        if (cursors[source_num].has_collation)
            throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

        source_chunk = chunk_allocator.alloc(std::move(chunk));
        cursors[source_num] = SortCursorImpl(source_chunk->getColumns(), description, source_num);
    }

    source_chunk->all_columns = cursors[source_num].all_columns;
    source_chunk->sort_columns = cursors[source_num].sort_columns;
}

void CollapsingSortedTransform::reportIncorrectData()
{
    std::stringstream s;
    s << "Incorrect data: number of rows with sign = 1 (" << count_positive
      << ") differs with number of rows with sign = -1 (" << count_negative
      << ") by more than one (for key: ";

    auto & sort_columns = *last_row.sort_columns;
    for (size_t i = 0, size = sort_columns.size(); i < size; ++i)
    {
        if (i != 0)
            s << ", ";
        s << applyVisitor(FieldVisitorToString(), (*sort_columns[i])[last_row.row_num]);
    }

    s << ").";

    /** Fow now we limit ourselves to just logging such situations,
      *  since the data is generated by external programs.
      * With inconsistent data, this is an unavoidable error that can not be easily corrected by admins. Therefore Warning.
      */
    LOG_WARNING(log, s.rdbuf());
}

void CollapsingSortedTransform::insertRow(RowRef & row)
{
    merged_data.insertRow(*row.all_columns, row.row_num, row.owned_chunk->getNumRows());
}

void CollapsingSortedTransform::insertRows()
{
    if (count_positive == 0 && count_negative == 0)
    {
        /// No input rows have been read.
        return;
    }

    if (last_is_positive || count_positive != count_negative)
    {
        if (count_positive <= count_negative)
        {
            insertRow(first_negative_row);

            if (out_row_sources_buf)
                current_row_sources[first_negative_pos].setSkipFlag(false);
        }

        if (count_positive >= count_negative)
        {
            insertRow(last_positive_row);

            if (out_row_sources_buf)
                current_row_sources[last_positive_pos].setSkipFlag(false);
        }

        if (!(count_positive == count_negative || count_positive + 1 == count_negative || count_positive == count_negative + 1))
        {
            if (count_incorrect_data < MAX_ERROR_MESSAGES)
                reportIncorrectData();
            ++count_incorrect_data;
        }
    }

    first_negative_row.clear();
    last_positive_row.clear();

    if (out_row_sources_buf)
        out_row_sources_buf->write(
                reinterpret_cast<const char *>(current_row_sources.data()),
                current_row_sources.size() * sizeof(RowSourcePart));
}

void CollapsingSortedTransform::work()
{
    merge();
    prepareOutputChunk(merged_data);
}

void CollapsingSortedTransform::merge()
{
    /// Take rows in required order and put them into `merged_data`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        auto current = queue.current();
        Int8 sign = assert_cast<const ColumnInt8 &>(*current->all_columns[sign_column_number]).getData()[current->pos];

        RowRef current_row;
        setRowRef(current_row, current);

        if (last_row.empty())
            setRowRef(last_row, current);

        bool key_differs = !last_row.hasEqualSortColumnsWith(current_row);

        /// if there are enough rows and the last one is calculated completely
        if (key_differs && merged_data.hasEnoughRows())
            return;

        if (key_differs)
        {
            /// We write data for the previous primary key.
            insertRows();

            current_row.swap(last_row);

            count_negative = 0;
            count_positive = 0;

            current_pos = 0;
            first_negative_pos = 0;
            last_positive_pos = 0;
            current_row_sources.resize(0);
        }

        /// Initially, skip all rows. On insert, unskip "corner" rows.
        if (out_row_sources_buf)
            current_row_sources.emplace_back(current.impl->order, true);

        if (sign == 1)
        {
            ++count_positive;
            last_is_positive = true;

            setRowRef(last_positive_row, current);
            last_positive_pos = current_pos;
        }
        else if (sign == -1)
        {
            if (!count_negative)
            {
                setRowRef(first_negative_row, current);
                first_negative_pos = current_pos;
            }

            ++count_negative;
            last_is_positive = false;
        }
        else
            throw Exception("Incorrect data: Sign = " + toString(sign) + " (must be 1 or -1).",
                            ErrorCodes::INCORRECT_DATA);

        ++current_pos;

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            /// We take next block from the corresponding source, if there is one.
            queue.removeTop();
            requestDataForInput(current.impl->order);
            return;
        }
    }

    insertRows();
    is_finished = true;
}

}
