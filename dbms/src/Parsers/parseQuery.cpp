#include <DB/Parsers/parseQuery.h>


namespace DB
{

/** Из позиции в (возможно многострочном) запросе получить номер строки и номер столбца в строке.
  * Используется в сообщении об ошибках.
  */
static inline std::pair<size_t, size_t> getLineAndCol(IParser::Pos begin, IParser::Pos pos)
{
	size_t line = 0;

	IParser::Pos nl;
	while (nullptr != (nl = reinterpret_cast<IParser::Pos>(memchr(begin, '\n', pos - begin))))
	{
		++line;
		begin = nl + 1;
	}

	/// Нумеруются с единицы.
	return { line + 1, pos - begin + 1 };
}


static std::string getSyntaxErrorMessage(
	IParser::Pos begin,
	IParser::Pos end,
	IParser::Pos max_parsed_pos,
	Expected expected,
	const std::string & description)
{
	std::stringstream message;

	message << "Syntax error";

	if (!description.empty())
		message << " (" << description << ")";

	message << ": failed at position " << (max_parsed_pos - begin + 1);

	/// Если запрос многострочный.
	IParser::Pos nl = reinterpret_cast<IParser::Pos>(memchr(begin, '\n', end - begin));
	if (nullptr != nl && nl + 1 != end)
	{
		size_t line = 0;
		size_t col = 0;
		std::tie(line, col) = getLineAndCol(begin, max_parsed_pos);

		message << " (line " << line << ", col " << col << ")";
	}

	message << ": " << std::string(max_parsed_pos, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - max_parsed_pos))
		<< ", expected " << expected << ".";

	return message.str();
}


ASTPtr tryParseQuery(IParser & parser, IParser::Pos begin, IParser::Pos end, std::string & out_error_message, const std::string & description)
{
	if (begin == end || *begin == ';')
	{
		out_error_message = "Empty query";
		return nullptr;
	}

	Expected expected = "";
	IParser::Pos pos = begin;
	IParser::Pos max_parsed_pos = pos;

	ASTPtr res;
	bool parse_res = parser.parse(pos, end, res, max_parsed_pos, expected);

	/// Распарсенный запрос должен заканчиваться на конец входных данных или на точку с запятой.
	if (!parse_res || (pos != end && *pos != ';'))
	{
		out_error_message = getSyntaxErrorMessage(begin, end, max_parsed_pos, expected, description);
		return nullptr;
	}

	return res;
}


ASTPtr parseQuery(IParser & parser, IParser::Pos begin, IParser::Pos end, const std::string & description)
{
	std::string error_message;
	ASTPtr res = tryParseQuery(parser, begin, end, error_message, description);

	if (res)
		return res;

	throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);
}

}
