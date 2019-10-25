#include <IO/ReadHelpers.h>

#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

enum
{
    UNKNOWN_FIELD = size_t(-1),
    NESTED_FIELD = size_t(-2)
};

}


JSONEachRowRowInputFormat::JSONEachRowRowInputFormat(
    ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, std::move(params_)), format_settings(format_settings_), name_map(header_.columns())
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(in);

    size_t num_columns = getPort().getHeader().columns();
    for (size_t i = 0; i < num_columns; ++i)
    {
        const String & column_name = columnName(i);
        name_map[column_name] = i;        /// NOTE You could place names more cache-locally.
        if (format_settings_.import_nested_json)
        {
            const auto splitted = Nested::splitName(column_name);
            if (!splitted.second.empty())
            {
                const StringRef table_name(column_name.data(), splitted.first.size());
                name_map[table_name] = NESTED_FIELD;
            }
        }
    }

    prev_positions.resize(num_columns);
}

const String & JSONEachRowRowInputFormat::columnName(size_t i) const
{
    return getPort().getHeader().getByPosition(i).name;
}

inline size_t JSONEachRowRowInputFormat::columnIndex(const StringRef & name, size_t key_index)
{
    /// Optimization by caching the order of fields (which is almost always the same)
    /// and a quick check to match the next expected field, instead of searching the hash table.

    if (prev_positions.size() > key_index
        && prev_positions[key_index]
        && name == *lookupResultGetKey(prev_positions[key_index]))
    {
        return *lookupResultGetMapped(prev_positions[key_index]);
    }
    else
    {
        const auto it = name_map.find(name);

        if (it)
        {
            if (key_index < prev_positions.size())
                prev_positions[key_index] = it;

            return *lookupResultGetMapped(it);
        }
        else
            return UNKNOWN_FIELD;
    }
}

/** Read the field name and convert it to column name
  *  (taking into account the current nested name prefix)
  * Resulting StringRef is valid only before next read from buf.
  */
StringRef JSONEachRowRowInputFormat::readColumnName(ReadBuffer & buf)
{
    // This is just an optimization: try to avoid copying the name into current_column_name

    if (nested_prefix_length == 0 && buf.position() + 1 < buf.buffer().end())
    {
        char * next_pos = find_first_symbols<'\\', '"'>(buf.position() + 1, buf.buffer().end());

        if (next_pos != buf.buffer().end() && *next_pos != '\\')
        {
            /// The most likely option is that there is no escape sequence in the key name, and the entire name is placed in the buffer.
            assertChar('"', buf);
            StringRef res(buf.position(), next_pos - buf.position());
            buf.position() = next_pos + 1;
            return res;
        }
    }

    current_column_name.resize(nested_prefix_length);
    readJSONStringInto(current_column_name, buf);
    return current_column_name;
}


static inline void skipColonDelimeter(ReadBuffer & istr)
{
    skipWhitespaceIfAny(istr);
    assertChar(':', istr);
    skipWhitespaceIfAny(istr);
}

void JSONEachRowRowInputFormat::skipUnknownField(const StringRef & name_ref)
{
    if (!format_settings.skip_unknown_fields)
        throw Exception("Unknown field found while parsing JSONEachRow format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

    skipJSONField(in, name_ref);
}

void JSONEachRowRowInputFormat::readField(size_t index, MutableColumns & columns)
{
    if (seen_columns[index])
        throw Exception("Duplicate field found while parsing JSONEachRow format: " + columnName(index), ErrorCodes::INCORRECT_DATA);

    try
    {
        seen_columns[index] = read_columns[index] = true;
        const auto & type = getPort().getHeader().getByPosition(index).type;
        if (format_settings.null_as_default && !type->isNullable())
            read_columns[index] = DataTypeNullable::deserializeTextJSON(*columns[index], in, format_settings, type);
        else
            type->deserializeAsTextJSON(*columns[index], in, format_settings);
    }
    catch (Exception & e)
    {
        e.addMessage("(while read the value of key " + columnName(index) + ")");
        throw;
    }
}

inline bool JSONEachRowRowInputFormat::advanceToNextKey(size_t key_index)
{
    skipWhitespaceIfAny(in);

    if (in.eof())
        throw Exception("Unexpected end of stream while parsing JSONEachRow format", ErrorCodes::CANNOT_READ_ALL_DATA);
    else if (*in.position() == '}')
    {
        ++in.position();
        return false;
    }

    if (key_index > 0)
    {
        assertChar(',', in);
        skipWhitespaceIfAny(in);
    }
    return true;
}

void JSONEachRowRowInputFormat::readJSONObject(MutableColumns & columns)
{
    assertChar('{', in);

    for (size_t key_index = 0; advanceToNextKey(key_index); ++key_index)
    {
        StringRef name_ref = readColumnName(in);
        const size_t column_index = columnIndex(name_ref, key_index);

        if (unlikely(ssize_t(column_index) < 0))
        {
            /// name_ref may point directly to the input buffer
            /// and input buffer may be filled with new data on next read
            /// If we want to use name_ref after another reads from buffer, we must copy it to temporary string.

            current_column_name.assign(name_ref.data, name_ref.size);
            name_ref = StringRef(current_column_name);

            skipColonDelimeter(in);

            if (column_index == UNKNOWN_FIELD)
                skipUnknownField(name_ref);
            else if (column_index == NESTED_FIELD)
                readNestedData(name_ref.toString(), columns);
            else
                throw Exception("Logical error: illegal value of column_index", ErrorCodes::LOGICAL_ERROR);
        }
        else
        {
            skipColonDelimeter(in);
            readField(column_index, columns);
        }
    }
}

void JSONEachRowRowInputFormat::readNestedData(const String & name, MutableColumns & columns)
{
    current_column_name = name;
    current_column_name.push_back('.');
    nested_prefix_length = current_column_name.size();
    readJSONObject(columns);
    nested_prefix_length = 0;
}


bool JSONEachRowRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    skipWhitespaceIfAny(in);

    /// We consume ;, or \n before scanning a new row, instead scanning to next row at the end.
    /// The reason is that if we want an exact number of rows read with LIMIT x
    /// from a streaming table engine with text data format, like File or Kafka
    /// then seeking to next ;, or \n would trigger reading of an extra row at the end.

    /// Semicolon is added for convenience as it could be used at end of INSERT query.
    if (!in.eof() && (*in.position() == ',' || *in.position() == ';'))
        ++in.position();

    skipWhitespaceIfAny(in);
    if (in.eof())
        return false;

    size_t num_columns = columns.size();

    read_columns.assign(num_columns, false);
    seen_columns.assign(num_columns, false);

    nested_prefix_length = 0;
    readJSONObject(columns);

    auto & header = getPort().getHeader();
    /// Fill non-visited columns with the default values.
    for (size_t i = 0; i < num_columns; ++i)
        if (!seen_columns[i])
            header.getByPosition(i).type->insertDefaultInto(*columns[i]);

    /// return info about defaults set
    ext.read_columns = read_columns;
    return true;
}


void JSONEachRowRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(in);
}


void registerInputFormatProcessorJSONEachRow(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("JSONEachRow", [](
        ReadBuffer & buf,
        const Block & sample,
        const Context &,
        IRowInputFormat::Params params,
        const FormatSettings & settings)
    {
        return std::make_shared<JSONEachRowRowInputFormat>(buf, sample, std::move(params), settings);
    });
}

bool fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, DB::Memory<> & memory, size_t & used_size, size_t min_chunk_size)
{
    skipWhitespaceIfAny(in);
    char * begin_pos = in.position();
    size_t balance = 0;
    bool quotes = false;
    memory.resize(min_chunk_size);
    while (!eofWithSavingBufferState(in, memory, used_size, begin_pos)
           && (balance || used_size + static_cast<size_t>(in.position() - begin_pos) < min_chunk_size))
    {
        if (quotes)
        {
            in.position() = find_first_symbols<'\\', '"'>(in.position(), in.buffer().end());
            if (in.position() == in.buffer().end())
                continue;
            if (*in.position() == '\\')
            {
                ++in.position();
                if (!eofWithSavingBufferState(in, memory, used_size, begin_pos))
                    ++in.position();
            }
            else if (*in.position() == '"')
            {
                ++in.position();
                quotes = false;
            }
        }
        else
        {
            in.position() = find_first_symbols<'{', '}', '\\', '"'>(in.position(), in.buffer().end());
            if (in.position() == in.buffer().end())
                continue;
            if (*in.position() == '{')
            {
                ++balance;
                ++in.position();
            }
            else if (*in.position() == '}')
            {
                --balance;
                ++in.position();
            }
            else if (*in.position() == '\\')
            {
                ++in.position();
                if (!eofWithSavingBufferState(in, memory, used_size, begin_pos))
                    ++in.position();
            }
            else if (*in.position() == '"')
            {
                quotes = true;
                ++in.position();
            }
        }
    }
    eofWithSavingBufferState(in, memory, used_size, begin_pos, true);
    return true;
}

void registerFileSegmentationEngineJSONEachRow(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("JSONEachRow", &fileSegmentationEngineJSONEachRowImpl);
}

}
