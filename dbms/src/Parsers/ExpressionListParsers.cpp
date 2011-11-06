#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>

#include <DB/Parsers/ExpressionListParsers.h>


namespace DB
{


bool ParserList::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	bool first = true;
	ParserWhiteSpaceOrComments ws;

	ASTExpressionList * list = new ASTExpressionList;
	node = list;

	while (1)
	{
		if (first)
		{
			ASTPtr elem;
			if (!elem_parser->parse(pos, end, elem, expected))
				break;

			list->children.push_back(elem);
		}
		else
		{
			ws.ignore(pos, end);
			if (!separator_parser->ignore(pos, end, expected))
				break;
			ws.ignore(pos, end);

			ASTPtr elem;
			if (!elem_parser->parse(pos, end, elem, expected))
				return false;

			list->children.push_back(elem);
		}

		first = false;
	}

	if (!allow_empty && first)
		return false;

	return true;
}
	

bool ParserLeftAssociativeBinaryOperatorList::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	bool first = true;
	ParserWhiteSpaceOrComments ws;

	while (1)
	{
		if (first)
		{
			ASTPtr elem;
			if (!elem_parser->parse(pos, end, elem, expected))
				return false;

			node = elem;
		}
		else
		{
			ws.ignore(pos, end);

			/// пробуем найти какой-нибудь из допустимых операторов
			Pos begin = pos;
			Operators_t::const_iterator it;
			for (it = operators.begin(); it != operators.end(); ++it)
			{
				ParserString op(it->first, true, true);
				if (op.ignore(pos, end, expected))
					break;
			}

			if (it == operators.end())
				break;

			ws.ignore(pos, end);

			/// функция, соответствующая оператору
			ASTFunction * p_function = new ASTFunction;
			ASTFunction & function = *p_function;
			ASTPtr function_node = p_function;

			/// аргументы функции
			ASTExpressionList * p_exp_list = new ASTExpressionList;
			ASTExpressionList & exp_list = *p_exp_list;
			ASTPtr exp_list_node = p_exp_list;

			ASTPtr elem;
			if (!elem_parser->parse(pos, end, elem, expected))
				return false;

			/// первым аргументом функции будет предыдущий элемент, вторым - следующий
			function.range.first = begin;
			function.range.second = pos;
			function.name = it->second;
			function.arguments = exp_list_node;
			function.children.push_back(exp_list_node);

			exp_list.children.push_back(node);
			exp_list.children.push_back(elem);
			exp_list.range.second = pos;

			/** специальное исключение для оператора доступа к элементу массива x[y], который
				* содержит инфиксную часть '[' и суффиксную ']' (задаётся в виде '[')
				*/
			if (it->first == "[")
			{
				ParserString rest_p("]");
			
				ws.ignore(pos, end);
				if (!rest_p.ignore(pos, end, expected))
					return false;
			}

			node = function_node;
		}

		first = false;
	}

	return true;
}


bool ParserPrefixUnaryOperatorExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	ParserWhiteSpaceOrComments ws;

	/// пробуем найти какой-нибудь из допустимых операторов
	Pos begin = pos;
	Operators_t::const_iterator it;
	for (it = operators.begin(); it != operators.end(); ++it)
	{
		ParserString op(it->first, true, true);
		if (op.ignore(pos, end, expected))
			break;
	}

	ws.ignore(pos, end);

	ASTPtr elem;
	if (!elem_parser->parse(pos, end, elem, expected))
		return false;

	if (it == operators.end())
		node = elem;
	else
	{
		/// функция, соответствующая оператору
		ASTFunction * p_function = new ASTFunction;
		ASTFunction & function = *p_function;
		ASTPtr function_node = p_function;

		/// аргументы функции
		ASTExpressionList * p_exp_list = new ASTExpressionList;
		ASTExpressionList & exp_list = *p_exp_list;
		ASTPtr exp_list_node = p_exp_list;

		function.range.first = begin;
		function.range.second = pos;
		function.name = it->second;
		function.arguments = exp_list_node;
		function.children.push_back(exp_list_node);

		exp_list.children.push_back(elem);
		exp_list.range.second = pos;

		node = function_node;
	}

	return true;
}


ParserAccessExpression::ParserAccessExpression()
	: elem_parser(new ParserExpressionElement),
	operator_parser(boost::assign::map_list_of
			(".", 	"tupleElement")
			("[", 	"arrayElement"),
		elem_parser)
{
}


ParserExpressionWithOptionalAlias::ParserExpressionWithOptionalAlias()
	: impl(new ParserWithOptionalAlias(new ParserLogicalOrExpression))
{
}


bool ParserExpressionList::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	return ParserList(new ParserExpressionWithOptionalAlias, new ParserString(",")).parse(pos, end, node, expected);
}


bool ParserNotEmptyExpressionList::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	return nested_parser.parse(pos, end, node, expected)
		&& !dynamic_cast<ASTExpressionList &>(*node).children.empty();
}


bool ParserOrderByExpressionList::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	return ParserList(new ParserOrderByElement, new ParserString(",")).parse(pos, end, node, expected);
}


}
