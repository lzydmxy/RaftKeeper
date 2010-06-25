#include <errno.h>
#include <cstdlib>

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>

#include <DB/Parsers/ExpressionElementParsers.h>


namespace DB
{


bool ParserArray::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;
	ASTPtr contents_node;
	ParserString open("["), close("]");
	ParserExpressionList contents;
	ParserWhiteSpaceOrComments ws;

	if (!open.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);
	contents.parse(pos, end, contents_node, expected);
	ws.ignore(pos, end);

	if (!close.ignore(pos, end, expected))
		return false;

	ASTFunction * function_node = new ASTFunction(StringRange(begin, pos));
	function_node->name = "array";
	function_node->arguments = contents_node;
	node = function_node;

	return true;
}


bool ParserParenthesisExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;
	ASTPtr contents_node;
	ParserString open("("), close(")");
	ParserExpressionList contents;
	ParserWhiteSpaceOrComments ws;

	if (!open.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);
	contents.parse(pos, end, contents_node, expected);
	ws.ignore(pos, end);

	if (!close.ignore(pos, end, expected))
		return false;

	ASTExpressionList & expr_list = dynamic_cast<ASTExpressionList &>(*contents_node);

	/// пустое выражение в скобках недопустимо
	if (expr_list.children.empty())
	{
		expected = "not empty list of expressions in parenthesis";
		return false;
	}

	if (expr_list.children.size() == 1)
	{
		node = expr_list.children.front();
	}
	else
	{
		ASTFunction * function_node = new ASTFunction(StringRange(begin, pos));
		function_node->name = "tuple";
		function_node->arguments = contents_node;
		node = function_node;
	}

	return true;
}
	

bool ParserIdentifier::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;

	while (pos != end
		&& ((*pos >= 'a' && *pos <= 'z')
			|| (*pos >= 'A' && *pos <= 'Z')
			|| (*pos == '_')
			|| (pos != begin && *pos >= '0' && *pos <= '9')))
		++pos;

	if (pos != begin)
	{
		node = new ASTIdentifier(StringRange(begin, pos), String(begin, pos - begin));
		return true;
	}
	else
		return false;
}


bool ParserFunction::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;
	ASTPtr contents_node;

	ParserIdentifier id_parser;
	ParserString open("("), close(")");
	ParserExpressionList contents;
	ParserWhiteSpaceOrComments ws;

	ASTPtr identifier = new ASTIdentifier;
	ASTPtr expr_list = new ASTExpressionList;

	if (!id_parser.parse(pos, end, identifier, expected))
		return false;

	ws.ignore(pos, end);

	if (!open.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);
	contents.parse(pos, end, expr_list, expected);
	ws.ignore(pos, end);

	if (!close.ignore(pos, end, expected))
		return false;

	ASTFunction * function_node = new ASTFunction(StringRange(begin, pos));
	function_node->name = dynamic_cast<ASTIdentifier &>(*identifier).name;
	function_node->arguments = expr_list;
	node = function_node;
	return true;
}


bool ParserNull::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;
	ParserString nested_parser("NULL", true);
	if (nested_parser.parse(pos, end, node, expected))
	{
		node = new ASTLiteral(StringRange(StringRange(begin, pos)), Null());
		return true;
	}
	else
		return false;
}


bool ParserNumber::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Field res;

	Pos begin = pos;
	if (pos == end)
		return false;

	Float64 float_value = std::strtod(pos, const_cast<char**>(&pos));
	if (pos == begin || errno == ERANGE)
	{
		expected = "number (this cause range error)";
		return false;
	}
	res = float_value;

	/// попробуем использовать более точный тип - UInt64 или Int64

	Pos pos_integer = begin;
	if (float_value < 0)
	{
		Int64 int_value = std::strtoll(pos_integer, const_cast<char**>(&pos_integer), 0);
		if (pos_integer == pos && errno != ERANGE)
			res = int_value;
	}
	else
	{
		UInt64 uint_value = std::strtoull(pos_integer, const_cast<char**>(&pos_integer), 0);
		if (pos_integer == pos && errno != ERANGE)
			res = uint_value;
	}

	node = new ASTLiteral(StringRange(begin, pos), res);
	return true;
}


bool ParserStringLiteral::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;
	String s;

	if (pos == end || *pos != '\'')
	{
		expected = "opening single quote";
		return false;
	}

	++pos;

	while (pos != end)
	{
		size_t bytes = 0;
		for (; pos + bytes != end; ++bytes)
			if (pos[bytes] == '\\' || pos[bytes] == '\'')
				break;

		s.append(pos, bytes);
		pos += bytes;

		if (*pos == '\'')
		{
			++pos;
			node = new ASTLiteral(StringRange(begin, pos), s);
			return true;
		}

		if (*pos == '\\')
		{
			++pos;
			if (pos == end)
			{
				expected = "escape sequence";
				return false;
			}
			s += parseEscapeSequence(*pos);
			++pos;
		}
	}

	expected = "closing single quote";
	return false;
}


bool ParserLiteral::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;

	ParserNull null_p;
	ParserNumber num_p;
	ParserStringLiteral str_p;

	if (null_p.parse(pos, end, node, expected))
		return true;
	pos = begin;

	if (num_p.parse(pos, end, node, expected))
		return true;
	pos = begin;

	if (str_p.parse(pos, end, node, expected))
		return true;
	pos = begin;

	expected = "literal: one of NULL, number, single quoted string";
	return false;
}


bool ParserExpressionElement::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;

	ParserParenthesisExpression paren_p;
	ParserArray array_p;
	ParserLiteral lit_p;
	ParserFunction fun_p;
	ParserIdentifier id_p;

	if (paren_p.parse(pos, end, node, expected))
		return true;
	pos = begin;

	if (array_p.parse(pos, end, node, expected))
		return true;
	pos = begin;

	if (lit_p.parse(pos, end, node, expected))
		return true;
	pos = begin;

	if (fun_p.parse(pos, end, node, expected))
		return true;
	pos = begin;

	if (id_p.parse(pos, end, node, expected))
		return true;
	pos = begin;

	expected = "expression element: one of array, literal, function, identifier, parenthised expression";
	return false;
}


}

