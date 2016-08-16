#include <DB/IO/WriteHelpers.h>
#include <DB/IO/WriteBufferValidUTF8.h>
#include <DB/DataStreams/XMLRowOutputStream.h>


namespace DB
{

XMLRowOutputStream::XMLRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
	: dst_ostr(ostr_)
{
	NamesAndTypesList columns(sample_.getColumnsList());
	fields.assign(columns.begin(), columns.end());
	field_tag_names.resize(sample_.columns());

	bool have_non_numeric_columns = false;
	for (size_t i = 0; i < sample_.columns(); ++i)
	{
		if (!sample_.unsafeGetByPosition(i).type->isNumeric())
			have_non_numeric_columns = true;

		/// В качестве имён элементов будем использовать имя столбца, если оно имеет допустимый вид, или "field", иначе.
		/// Условие, приведённое ниже, более строгое, чем того требует стандарт XML.
		bool is_column_name_suitable = true;
		const char * begin = fields[i].name.data();
		const char * end = begin + fields[i].name.size();
		for (const char * pos = begin; pos != end; ++pos)
		{
			char c = *pos;
			if (!( isAlphaASCII(c)
				|| (pos != begin && isNumericASCII(c))
				|| c == '_'
				|| c == '-'
				|| c == '.'))
			{
				is_column_name_suitable = false;
				break;
			}
		}

		field_tag_names[i] = is_column_name_suitable
			? fields[i].name
			: "field";
	}

	if (have_non_numeric_columns)
	{
		validating_ostr.reset(new WriteBufferValidUTF8(dst_ostr));
		ostr = validating_ostr.get();
	}
	else
		ostr = &dst_ostr;
}


void XMLRowOutputStream::writePrefix()
{
	writeCString("<?xml version='1.0' encoding='UTF-8' ?>\n", *ostr);
	writeCString("<result>\n", *ostr);
	writeCString("\t<meta>\n", *ostr);
	writeCString("\t\t<columns>\n", *ostr);

	for (size_t i = 0; i < fields.size(); ++i)
	{
		writeCString("\t\t\t<column>\n", *ostr);

		writeCString("\t\t\t\t<name>", *ostr);
		writeXMLString(fields[i].name, *ostr);
		writeCString("</name>\n", *ostr);
		writeCString("\t\t\t\t<type>", *ostr);
		writeXMLString(fields[i].type->getName(), *ostr);
		writeCString("</type>\n", *ostr);

		writeCString("\t\t\t</column>\n", *ostr);
	}

	writeCString("\t\t</columns>\n", *ostr);
	writeCString("\t</meta>\n", *ostr);
	writeCString("\t<data>\n", *ostr);
}


void XMLRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
	writeCString("\t\t\t<", *ostr);
	writeString(field_tag_names[field_number], *ostr);
	writeCString(">", *ostr);
	type.serializeTextXML(column, row_num, *ostr);
	writeCString("</", *ostr);
	writeString(field_tag_names[field_number], *ostr);
	writeCString(">\n", *ostr);
	++field_number;
}


void XMLRowOutputStream::writeRowStartDelimiter()
{
	writeCString("\t\t<row>\n", *ostr);
}


void XMLRowOutputStream::writeRowEndDelimiter()
{
	writeCString("\t\t</row>\n", *ostr);
	field_number = 0;
	++row_count;
}


void XMLRowOutputStream::writeSuffix()
{
	writeCString("\t</data>\n", *ostr);

	writeTotals();
	writeExtremes();

	writeCString("\t<rows>", *ostr);
	writeIntText(row_count, *ostr);
	writeCString("</rows>\n", *ostr);

	writeRowsBeforeLimitAtLeast();

	writeCString("</result>\n", *ostr);
	ostr->next();
}

void XMLRowOutputStream::writeRowsBeforeLimitAtLeast()
{
	if (applied_limit)
	{
		writeCString("\t<rows_before_limit_at_least>", *ostr);
		writeIntText(rows_before_limit, *ostr);
		writeCString("</rows_before_limit_at_least>\n", *ostr);
	}
}

void XMLRowOutputStream::writeTotals()
{
	if (totals)
	{
		writeCString("\t<totals>\n", *ostr);

		size_t totals_columns = totals.columns();
		for (size_t i = 0; i < totals_columns; ++i)
		{
			const ColumnWithTypeAndName & column = totals.getByPosition(i);

			writeCString("\t\t<", *ostr);
			writeString(field_tag_names[i], *ostr);
			writeCString(">", *ostr);
			column.type->serializeTextXML(*column.column.get(), 0, *ostr);
			writeCString("</", *ostr);
			writeString(field_tag_names[i], *ostr);
			writeCString(">\n", *ostr);
		}

		writeCString("\t</totals>\n", *ostr);
	}
}


static void writeExtremesElement(const char * title, const Block & extremes, size_t row_num, const Names & field_tag_names, WriteBuffer & ostr)
{
	writeCString("\t\t<", ostr);
	writeCString(title, ostr);
	writeCString(">\n", ostr);

	size_t extremes_columns = extremes.columns();
	for (size_t i = 0; i < extremes_columns; ++i)
	{
		const ColumnWithTypeAndName & column = extremes.getByPosition(i);

		writeCString("\t\t\t<", ostr);
		writeString(field_tag_names[i], ostr);
		writeCString(">", ostr);
		column.type->serializeTextXML(*column.column.get(), row_num, ostr);
		writeCString("</", ostr);
		writeString(field_tag_names[i], ostr);
		writeCString(">\n", ostr);
	}

	writeCString("\t\t</", ostr);
	writeCString(title, ostr);
	writeCString(">\n", ostr);
}

void XMLRowOutputStream::writeExtremes()
{
	if (extremes)
	{
		writeCString("\t<extremes>\n", *ostr);
		writeExtremesElement("min", extremes, 0, field_tag_names, *ostr);
		writeExtremesElement("max", extremes, 1, field_tag_names, *ostr);
		writeCString("\t</extremes>\n", *ostr);
	}
}

}
