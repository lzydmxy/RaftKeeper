#include <Processors/Formats/Impl/TemplateRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/verbosePrintString.h>
#include <IO/Operators.h>
#include <DataTypes/DataTypeNothing.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_TEMPLATE_FORMAT;
extern const int ATTEMPT_TO_READ_AFTER_EOF;
extern const int CANNOT_READ_ALL_DATA;
extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
extern const int CANNOT_PARSE_QUOTED_STRING;
}


TemplateRowInputFormat::TemplateRowInputFormat(ReadBuffer & in_, const Block & header_, const Params & params_,
        const FormatSettings & settings_, bool ignore_spaces_)
    : RowInputFormatWithDiagnosticInfo(header_, in_, params_), buf(in_), data_types(header_.getDataTypes()),
    settings(settings_), ignore_spaces(ignore_spaces_)
{
    /// Parse format string for whole input
    static const String default_format("${data}");
    const String & format_str = settings.template_settings.format.empty() ? default_format : settings.template_settings.format;
    format = ParsedTemplateFormatString(format_str, [&](const String & partName) -> std::optional<size_t>
    {
        if (partName == "data")
            return 0;
        else if (partName.empty())      /// For skipping some values in prefix and suffix
            return {};
        throw Exception("invalid template format: unknown input part " + partName, ErrorCodes::INVALID_TEMPLATE_FORMAT);
    });

    /// Validate format string for whole input
    bool has_data = false;
    for (size_t i = 0; i < format.columnsCount(); ++i)
    {
        if (format.format_idx_to_column_idx[i])
        {
            if (has_data)
                throw Exception("${data} can occur only once", ErrorCodes::INVALID_TEMPLATE_FORMAT);
            if (format.formats[i] != ColumnFormat::None)
                throw Exception("invalid template format: ${data} must have empty or None serialization type", ErrorCodes::INVALID_TEMPLATE_FORMAT);
            has_data = true;
            format_data_idx = i;
        }
        else
        {
            if (format.formats[i] == ColumnFormat::Xml || format.formats[i] == ColumnFormat::Raw)
                throw Exception("None, XML and Raw deserialization is not supported", ErrorCodes::INVALID_TEMPLATE_FORMAT);
        }
    }

    /// Parse format string for rows
    row_format = ParsedTemplateFormatString(settings.template_settings.row_format, [&](const String & colName) -> std::optional<size_t>
    {
        if (colName.empty())
            return {};
        return header_.getPositionByName(colName);
    });

    /// Validate format string for rows
    std::vector<UInt8> column_in_format(header_.columns(), false);
    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        if (row_format.formats[i] == ColumnFormat::Xml || row_format.formats[i] == ColumnFormat::Raw)
            throw Exception("invalid template format: None, XML and Raw deserialization is not supported", ErrorCodes::INVALID_TEMPLATE_FORMAT);

        if (row_format.format_idx_to_column_idx[i])
        {
            if (row_format.formats[i] == ColumnFormat::None)
                throw Exception("invalid template format: None, XML and Raw deserialization is not supported", ErrorCodes::INVALID_TEMPLATE_FORMAT);

            size_t col_idx = *row_format.format_idx_to_column_idx[i];
            if (column_in_format[col_idx])
                throw Exception("invalid template format: duplicate column " + header_.getColumnsWithTypeAndName()[col_idx].name,
                                ErrorCodes::INVALID_TEMPLATE_FORMAT);
            column_in_format[col_idx] = true;
        }
    }
}

void TemplateRowInputFormat::readPrefix()
{
    tryReadPrefixOrSuffix<void>(0, format_data_idx);
}

/// Asserts delimiters and skips fields in prefix or suffix.
/// tryReadPrefixOrSuffix<bool>(...) is used in checkForSuffix() to avoid throwing an exception after read of each row
/// (most likely false will be returned on first call of checkString(...))
template <typename ReturnType>
ReturnType TemplateRowInputFormat::tryReadPrefixOrSuffix(size_t input_part_beg, size_t input_part_end)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    skipSpaces();
    if constexpr (throw_exception)
        assertString(format.delimiters[input_part_beg], buf);
    else
    {
        if (likely(!checkString(format.delimiters[input_part_beg], buf)))
            return ReturnType(false);
    }

    while (input_part_beg < input_part_end)
    {
        skipSpaces();
        if constexpr (throw_exception)
            skipField(format.formats[input_part_beg]);
        else
        {
            try
            {
                skipField(format.formats[input_part_beg]);
            }
            catch (const Exception & e)
            {
                if (e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF &&
                    e.code() != ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE &&
                    e.code() != ErrorCodes::CANNOT_PARSE_QUOTED_STRING)
                    throw;
                /// If it's parsing error, then suffix is not found
                return ReturnType(false);
            }
        }
        ++input_part_beg;

        skipSpaces();
        if constexpr (throw_exception)
            assertString(format.delimiters[input_part_beg], buf);
        else
        {
            if (likely(!checkString(format.delimiters[input_part_beg], buf)))
                return ReturnType(false);
        }
    }

    if constexpr (!throw_exception)
        return ReturnType(true);
}

bool TemplateRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & extra)
{
    /// This function can be called again after it returned false
    if (unlikely(end_of_stream))
        return false;

    skipSpaces();

    if (unlikely(checkForSuffix()))
    {
        end_of_stream = true;
        return false;
    }

    updateDiagnosticInfo();

    if (likely(row_num != 1))
        assertString(settings.template_settings.row_between_delimiter, buf);

    extra.read_columns.assign(columns.size(), false);

    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        skipSpaces();
        assertString(row_format.delimiters[i], buf);
        skipSpaces();
        if (row_format.format_idx_to_column_idx[i])
        {
            size_t col_idx = *row_format.format_idx_to_column_idx[i];
            deserializeField(*data_types[col_idx], *columns[col_idx], row_format.formats[i]);
            extra.read_columns[col_idx] = true;
        }
        else
            skipField(row_format.formats[i]);

    }

    skipSpaces();
    assertString(row_format.delimiters.back(), buf);

    for (size_t i = 0; i < columns.size(); ++i)
        if (!extra.read_columns[i])
            data_types[i]->insertDefaultInto(*columns[i]);

    return true;
}

void TemplateRowInputFormat::deserializeField(const IDataType & type, IColumn & column, ColumnFormat col_format)
{
    try
    {
        switch (col_format)
        {
            case ColumnFormat::Escaped:
                type.deserializeAsTextEscaped(column, buf, settings);
                break;
            case ColumnFormat::Quoted:
                type.deserializeAsTextQuoted(column, buf, settings);
                break;
            case ColumnFormat::Csv:
                type.deserializeAsTextCSV(column, buf, settings);
                break;
            case ColumnFormat::Json:
                type.deserializeAsTextJSON(column, buf, settings);
                break;
            default:
                __builtin_unreachable();
        }
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throwUnexpectedEof();
        throw;
    }
}

void TemplateRowInputFormat::skipField(TemplateRowInputFormat::ColumnFormat col_format)
{
    String tmp;
    constexpr const char * field_name = "<SKIPPED COLUMN>";
    constexpr size_t field_name_len = 16;
    try
    {
        switch (col_format)
        {
            case ColumnFormat::None:
                /// Empty field, just skip spaces
                break;
            case ColumnFormat::Escaped:
                readEscapedString(tmp, buf);
                break;
            case ColumnFormat::Quoted:
                readQuotedString(tmp, buf);
                break;
            case ColumnFormat::Csv:
                readCSVString(tmp, buf, settings.csv);
                break;
            case ColumnFormat::Json:
                skipJSONField(buf, StringRef(field_name, field_name_len));
                break;
            default:
                __builtin_unreachable();
        }
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throwUnexpectedEof();
        throw;
    }
}

/// Returns true if all rows have been read i.e. there are only suffix and spaces (if ignore_spaces == true) before EOF.
/// Otherwise returns false
bool TemplateRowInputFormat::checkForSuffix()
{
    PeekableReadBufferCheckpoint checkpoint{buf};
    bool suffix_found = false;
    try
    {
        suffix_found = tryReadPrefixOrSuffix<bool>(format_data_idx + 1, format.columnsCount());
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF &&
            e.code() != ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE &&
            e.code() != ErrorCodes::CANNOT_PARSE_QUOTED_STRING)
            throw;
    }

    /// TODO better diagnostic in case of invalid suffix

    if (unlikely(suffix_found))
    {
        skipSpaces();
        if (buf.eof())
            return true;
    }

    buf.rollbackToCheckpoint();
    return false;
}

bool TemplateRowInputFormat::parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out)
{
    try
    {
        if (likely(row_num != 1))
            assertString(settings.template_settings.row_between_delimiter, buf);
    }
    catch (const DB::Exception &)
    {
        writeErrorStringForWrongDelimiter(out, "delimiter between rows", settings.template_settings.row_between_delimiter);

        return false;
    }
    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        skipSpaces();
        try
        {
            assertString(row_format.delimiters[i], buf);
        }
        catch (const DB::Exception &)
        {
            writeErrorStringForWrongDelimiter(out, "delimiter before field " + std::to_string(i), row_format.delimiters[i]);
            return false;
        }

        skipSpaces();
        if (row_format.format_idx_to_column_idx[i])
        {
            auto & header = getPort().getHeader();
            size_t col_idx = *row_format.format_idx_to_column_idx[i];
            if (!deserializeFieldAndPrintDiagnosticInfo(header.getByPosition(col_idx).name, data_types[col_idx],
                                                        *columns[col_idx], out, i))
            {
                out << "Maybe it's not possible to deserialize field " + std::to_string(i) +
                       " as " + ParsedTemplateFormatString::formatToString(row_format.formats[i]);
                return false;
            }
        }
        else
        {
            static const String skipped_column_str = "<SKIPPED COLUMN>";
            static const DataTypePtr skipped_column_type = std::make_shared<DataTypeNothing>();
            static const MutableColumnPtr skipped_column = skipped_column_type->createColumn();
            if (!deserializeFieldAndPrintDiagnosticInfo(skipped_column_str, skipped_column_type, *skipped_column, out, i))
                return false;
        }
    }

    skipSpaces();
    try
    {
        assertString(row_format.delimiters.back(), buf);
    }
    catch (const DB::Exception &)
    {
        writeErrorStringForWrongDelimiter(out, "delimiter after last field", row_format.delimiters.back());
        return false;
    }

    return true;
}

void TemplateRowInputFormat::writeErrorStringForWrongDelimiter(WriteBuffer & out, const String & description, const String & delim)
{
    out << "ERROR: There is no " << description << ": expected ";
    verbosePrintString(delim.data(), delim.data() + delim.size(), out);
    out << ", got ";
    if (buf.eof())
        out << "<End of stream>";
    else
        verbosePrintString(buf.position(), std::min(buf.position() + delim.size() + 10, buf.buffer().end()), out);
    out << '\n';
}

void TemplateRowInputFormat::tryDeserializeFiled(const DataTypePtr & type, IColumn & column, size_t input_position, ReadBuffer::Position & prev_pos,
                                                 ReadBuffer::Position & curr_pos)
{
    prev_pos = buf.position();
    if (row_format.format_idx_to_column_idx[input_position])
        deserializeField(*type, column, row_format.formats[input_position]);
    else
        skipField(row_format.formats[input_position]);
    curr_pos = buf.position();
}

bool TemplateRowInputFormat::isGarbageAfterField(size_t, ReadBuffer::Position)
{
    /// Garbage will be considered as wrong delimiter
    return false;
}

bool TemplateRowInputFormat::allowSyncAfterError() const
{
    return !row_format.delimiters.back().empty() || !settings.template_settings.row_between_delimiter.empty();
}

void TemplateRowInputFormat::syncAfterError()
{
    bool at_beginning_of_row_or_eof = false;
    while (!at_beginning_of_row_or_eof)
    {
        skipToNextDelimiterOrEof(row_format.delimiters.back());
        if (buf.eof())
        {
            end_of_stream = true;
            return;
        }
        buf.ignore(row_format.delimiters.back().size());

        skipSpaces();
        if (checkForSuffix())
            return;

        bool last_delimiter_in_row_found = !row_format.delimiters.back().empty();

        if (last_delimiter_in_row_found && checkString(settings.template_settings.row_between_delimiter, buf))
            at_beginning_of_row_or_eof = true;
        else
            skipToNextDelimiterOrEof(settings.template_settings.row_between_delimiter);

        if (buf.eof())
            at_beginning_of_row_or_eof = end_of_stream = true;
    }
    /// It can happen that buf.position() is not at the beginning of row
    /// if some delimiters is similar to row_format.delimiters.back() and row_between_delimiter.
    /// It will cause another parsing error.
}

/// Searches for delimiter in input stream and sets buffer position to the beginning of delimiter (if found) or EOF (if not)
void TemplateRowInputFormat::skipToNextDelimiterOrEof(const String & delimiter)
{
    if (delimiter.empty())
        return;

    while (!buf.eof())
    {
        void * pos = memchr(buf.position(), delimiter[0], buf.available());
        if (!pos)
        {
            buf.position() += buf.available();
            continue;
        }

        buf.position() = static_cast<ReadBuffer::Position>(pos);

        PeekableReadBufferCheckpoint checkpoint{buf};
        if (checkString(delimiter, buf))
            return;

        buf.rollbackToCheckpoint();
        ++buf.position();
    }
}

void TemplateRowInputFormat::throwUnexpectedEof()
{
    throw Exception("Unexpected EOF while parsing row " + std::to_string(row_num) + ". "
                    "Maybe last row has wrong format or input doesn't contain specified suffix before EOF.",
                    ErrorCodes::CANNOT_READ_ALL_DATA);
}


void registerInputFormatProcessorTemplate(FormatFactory & factory)
{
    for (bool ignore_spaces : {false, true})
    {
        factory.registerInputFormatProcessor(ignore_spaces ? "TemplateIgnoreSpaces" : "Template", [=](
                ReadBuffer & buf,
                const Block & sample,
                const Context &,
                IRowInputFormat::Params params,
                const FormatSettings & settings)
        {
            return std::make_shared<TemplateRowInputFormat>(buf, sample, std::move(params), settings, ignore_spaces);
        });
    }
}

}
