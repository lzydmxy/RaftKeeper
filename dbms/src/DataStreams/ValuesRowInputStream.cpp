#include <DB/IO/ReadHelpers.h>
#include <DB/Interpreters/evaluateConstantExpression.h>
#include <DB/Interpreters/convertFieldToType.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/DataStreams/ValuesRowInputStream.h>
#include <DB/Core/FieldVisitors.h>
#include <DB/Core/Block.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
	extern const int CANNOT_PARSE_QUOTED_STRING;
	extern const int CANNOT_PARSE_DATE;
	extern const int CANNOT_PARSE_DATETIME;
	extern const int CANNOT_READ_ARRAY_FROM_TEXT;
	extern const int CANNOT_PARSE_DATE;
	extern const int SYNTAX_ERROR;
	extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}


ValuesRowInputStream::ValuesRowInputStream(ReadBuffer & istr_, const Context & context_)
	: istr(istr_), context(context_)
{
	/// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
	skipBOMIfExists(istr);
}


bool ValuesRowInputStream::read(Block & block)
{
	size_t size = block.columns();

	skipWhitespaceIfAny(istr);

	if (istr.eof() || *istr.position() == ';')
		return false;

	/** Как правило, это обычный формат для потокового парсинга.
	  * Но в качестве исключения, поддерживается также обработка произвольных выражений вместо значений.
	  * Это очень неэффективно. Но если выражений нет, то оверхед отсутствует.
	  */
	ParserExpressionWithOptionalAlias parser(false);

	assertChar('(', istr);

	for (size_t i = 0; i < size; ++i)
	{
		skipWhitespaceIfAny(istr);

		char * prev_istr_position = istr.position();
		size_t prev_istr_bytes = istr.count() - istr.offset();

		auto & col = block.unsafeGetByPosition(i);

		bool rollback_on_exception = false;
		try
		{
			col.type.get()->deserializeTextQuoted(*col.column.get(), istr);
			rollback_on_exception = true;
			skipWhitespaceIfAny(istr);

			if (i != size - 1)
				assertChar(',', istr);
			else
				assertChar(')', istr);
		}
		catch (const Exception & e)
		{
			/** Обычный потоковый парсер не смог распарсить значение.
			  * Попробуем распарсить его SQL-парсером как константное выражение.
			  * Это исключительный случай.
			  */
			if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
				|| e.code() == ErrorCodes::CANNOT_PARSE_QUOTED_STRING
				|| e.code() == ErrorCodes::CANNOT_PARSE_DATE
				|| e.code() == ErrorCodes::CANNOT_PARSE_DATETIME
				|| e.code() == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT
				|| e.code() == ErrorCodes::CANNOT_PARSE_DATE)
			{
				/// TODO Работоспособность, если выражение не помещается целиком до конца буфера.

				/// Если начало значения уже не лежит в буфере.
				if (istr.count() - istr.offset() != prev_istr_bytes)
					throw;

				if (rollback_on_exception)
					col.column.get()->popBack(1);

				IDataType & type = *block.getByPosition(i).type;

				IParser::Pos pos = prev_istr_position;

				Expected expected = "";
				IParser::Pos max_parsed_pos = pos;

				ASTPtr ast;
				if (!parser.parse(pos, istr.buffer().end(), ast, max_parsed_pos, expected))
					throw Exception("Cannot parse expression of type " + type.getName() + " here: "
						+ String(prev_istr_position, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, istr.buffer().end() - prev_istr_position)),
						ErrorCodes::SYNTAX_ERROR);

				istr.position() = const_cast<char *>(max_parsed_pos);

				Field value = convertFieldToType(evaluateConstantExpression(ast, context), type);

				/// TODO После добавления поддержки NULL, добавить сюда проверку на data type is nullable.
				if (value.isNull())
					throw Exception("Expression returns value " + apply_visitor(FieldVisitorToString(), value)
						+ ", that is out of range of type " + type.getName()
						+ ", at: " + String(prev_istr_position, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, istr.buffer().end() - prev_istr_position)),
						ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE);

				col.column->insert(value);

				skipWhitespaceIfAny(istr);

				if (i != size - 1)
					assertChar(',', istr);
				else
					assertChar(')', istr);
			}
			else
				throw;
		}
	}

	skipWhitespaceIfAny(istr);
	if (!istr.eof() && *istr.position() == ',')
		++istr.position();

	return true;
}

}
