#include <DB/DataStreams/JSONCompactRowOutputStream.h>

#include <DB/IO/WriteHelpers.h>


namespace DB
{

JSONCompactRowOutputStream::JSONCompactRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool write_statistics_, bool force_quoting_64bit_integers_)
    : JSONRowOutputStream(ostr_, sample_, write_statistics_, force_quoting_64bit_integers_)
{
}


void JSONCompactRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeTextJSON(column, row_num, *ostr, force_quoting_64bit_integers);
    ++field_number;
}


void JSONCompactRowOutputStream::writeFieldDelimiter()
{
    writeCString(", ", *ostr);
}


void JSONCompactRowOutputStream::writeRowStartDelimiter()
{
    if (row_count > 0)
        writeCString(",\n", *ostr);
    writeCString("\t\t[", *ostr);
}


void JSONCompactRowOutputStream::writeRowEndDelimiter()
{
    writeChar(']', *ostr);
    field_number = 0;
    ++row_count;
}


void JSONCompactRowOutputStream::writeTotals()
{
    if (totals)
    {
        writeCString(",\n", *ostr);
        writeChar('\n', *ostr);
        writeCString("\t\"totals\": [", *ostr);

        size_t totals_columns = totals.columns();
        for (size_t i = 0; i < totals_columns; ++i)
        {
            if (i != 0)
                writeChar(',', *ostr);

            const ColumnWithTypeAndName & column = totals.safeGetByPosition(i);
            column.type->serializeTextJSON(*column.column.get(), 0, *ostr, force_quoting_64bit_integers);
        }

        writeChar(']', *ostr);
    }
}


static void writeExtremesElement(const char * title, const Block & extremes, size_t row_num, WriteBuffer & ostr, bool force_quoting_64bit_integers)
{
    writeCString("\t\t\"", ostr);
    writeCString(title, ostr);
    writeCString("\": [", ostr);

    size_t extremes_columns = extremes.columns();
    for (size_t i = 0; i < extremes_columns; ++i)
    {
        if (i != 0)
            writeChar(',', ostr);

        const ColumnWithTypeAndName & column = extremes.safeGetByPosition(i);
        column.type->serializeTextJSON(*column.column.get(), row_num, ostr, force_quoting_64bit_integers);
    }

    writeChar(']', ostr);
}

void JSONCompactRowOutputStream::writeExtremes()
{
    if (extremes)
    {
        writeCString(",\n", *ostr);
        writeChar('\n', *ostr);
        writeCString("\t\"extremes\":\n", *ostr);
        writeCString("\t{\n", *ostr);

        writeExtremesElement("min", extremes, 0, *ostr, force_quoting_64bit_integers);
        writeCString(",\n", *ostr);
        writeExtremesElement("max", extremes, 1, *ostr, force_quoting_64bit_integers);

        writeChar('\n', *ostr);
        writeCString("\t}", *ostr);
    }
}


}
