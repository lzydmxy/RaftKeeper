#include <DB/Storages/MergeTree/PKCondition.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Columns/ColumnSet.h>

namespace DB
{

PKCondition::PKCondition(ASTPtr query, const Context & context_, const NamesAndTypesList & all_columns, const SortDescription & sort_descr_)
	: sort_descr(sort_descr_)
{
	for (size_t i = 0; i < sort_descr.size(); ++i)
	{
		std::string name = sort_descr[i].column_name;
		if (!pk_columns.count(name))
			pk_columns[name] = i;
	}
	
	/** Вычисление выражений, зависящих только от констант.
	 * Чтобы индекс мог использоваться, если написано, например WHERE Date = toDate(now()).
	 */
	ExpressionActionsPtr expr_for_constant_folding = ExpressionAnalyzer(query, context_, all_columns).getConstActions();
	Block block_with_constants;
	
	/// В блоке должен быть хотя бы один столбец, чтобы у него было известно число строк.
	ColumnWithNameAndType dummy_column;
	dummy_column.name = "_dummy";
	dummy_column.type = new DataTypeUInt8;
	dummy_column.column = new ColumnConstUInt8(1, 0);
	block_with_constants.insert(dummy_column);
	
	expr_for_constant_folding->execute(block_with_constants);
	
	/// Преобразуем секцию WHERE в обратную польскую строку.
	ASTSelectQuery & select = dynamic_cast<ASTSelectQuery &>(*query);
	if (select.where_expression)
	{
		traverseAST(select.where_expression, block_with_constants);

		if (select.prewhere_expression)
		{
			traverseAST(select.prewhere_expression, block_with_constants);
			rpn.push_back(RPNElement(RPNElement::FUNCTION_AND));
		}
	}
	else if (select.prewhere_expression)
	{
		traverseAST(select.prewhere_expression, block_with_constants);
	}
	else
	{
		rpn.push_back(RPNElement(RPNElement::FUNCTION_UNKNOWN));
	}
}

bool PKCondition::addCondition(const String & column, const Range & range)
{
	if (!pk_columns.count(column))
		return false;
	rpn.push_back(RPNElement(RPNElement::FUNCTION_IN_RANGE, pk_columns[column], range));
	rpn.push_back(RPNElement(RPNElement::FUNCTION_AND));
	return true;
}

/** Получить значение константного выражения.
 * Вернуть false, если выражение не константно.
 */
static bool getConstant(ASTPtr & expr, Block & block_with_constants, Field & value)
{
	String column_name = expr->getColumnName();
	
	if (ASTLiteral * lit = dynamic_cast<ASTLiteral *>(&*expr))
	{
		/// литерал
		value = lit->value;
		return true;
	}
	else if (block_with_constants.has(column_name) && block_with_constants.getByName(column_name).column->isConst())
	{
		/// выражение, вычислившееся в константу
		value = (*block_with_constants.getByName(column_name).column)[0];
		return true;
	}
	else
		return false;
}

void PKCondition::traverseAST(ASTPtr & node, Block & block_with_constants)
{
	RPNElement element;
	
	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*node))
	{
		if (operatorFromAST(func, element))
		{
			ASTs & args = dynamic_cast<ASTExpressionList &>(*func->arguments).children;
			for (size_t i = 0; i < args.size(); ++i)
			{
				traverseAST(args[i], block_with_constants);
				if (i)
					rpn.push_back(element);
			}
			
			return;
		}
	}
	
	if (!atomFromAST(node, block_with_constants, element))
	{
		element.function = RPNElement::FUNCTION_UNKNOWN;
	}
	
	rpn.push_back(element);
}

bool PKCondition::atomFromAST(ASTPtr & node, Block & block_with_constants, RPNElement & out)
{
	/// Фнукции < > = != <= >= in , у которых один агрумент константа, другой - один из столбцов первичного ключа.
	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*node))
	{
		ASTs & args = dynamic_cast<ASTExpressionList &>(*func->arguments).children;
		
		if (args.size() != 2)
			return false;
		
		/// Если true, слева константа.
		bool inverted;
		size_t column;
		Field value;

		if (pk_columns.count(args[0]->getColumnName()) && getConstant(args[1], block_with_constants, value))
		{
			inverted = false;
			column = pk_columns[args[0]->getColumnName()];
		}
		else if (pk_columns.count(args[1]->getColumnName()) && getConstant(args[0], block_with_constants, value))
		{
			inverted = true;
			column = pk_columns[args[1]->getColumnName()];
		}
		/// для In, notIn
		else if (pk_columns.count(args[0]->getColumnName()) && dynamic_cast<DB::ColumnSet *>(args[1]))
		{
			column = pk_columns[args[1]->getColumnName()];
		}
		else
			return false;
		
		std::string func_name = func->name;
		
		/// Заменим <const> <sign> <column> на <column> <-sign> <const>
		if (inverted)
		{
			if (func_name == "less")
				func_name = "greater";
			else if (func_name == "greater")
				func_name = "less";
			else if (func_name == "greaterOrEquals")
				func_name = "lessOrEquals";
			else if (func_name == "lessOrEquals")
				func_name = "greaterOrEquals";
		}
		
		out.function = RPNElement::FUNCTION_IN_RANGE;
		out.key_column = column;
		
		if (func->name == "notEquals")
		{
			out.function = RPNElement::FUNCTION_NOT_IN_RANGE;
			out.range = Range(value);
		}
		else if (func->name == "equals")
			out.range = Range(value);
		else if (func->name == "less")
			out.range = Range::createRightBounded(value, false);
		else if (func->name == "greater")
			out.range = Range::createLeftBounded(value, false);
		else if (func->name == "lessOrEquals")
			out.range = Range::createRightBounded(value, true);
		else if (func->name == "greaterOrEquals")
			out.range = Range::createLeftBounded(value, true);
		else if (func->name == "in" || func->name == "notIn")
		{
			out.function = RPNElement::FUNCTION_IN_SET;
			out.set = Set(args[1]);
		}
		else
			return false;
		
		return true;
	}
	
	return false;
}

bool PKCondition::operatorFromAST(ASTFunction * func, RPNElement & out)
{
	/// Фнукции AND, OR, NOT.
	ASTs & args = dynamic_cast<ASTExpressionList &>(*func->arguments).children;
	
	if (func->name == "not")
	{
		if (args.size() != 1)
			return false;
		
		out.function = RPNElement::FUNCTION_NOT;
	}
	else
	{
		if (func->name == "and")
			out.function = RPNElement::FUNCTION_AND;
		else if (func->name == "or")
			out.function = RPNElement::FUNCTION_OR;
		else
			return false;
	}
	
	return true;
}

String PKCondition::toString()
{
	String res;
	for (size_t i = 0; i < rpn.size(); ++i)
	{
		if (i)
			res += ", ";
		res += rpn[i].toString();
	}
	return res;
}

/// Множество значений булевой переменной. То есть два булевых значения: может ли быть true, может ли быть false.
struct BoolMask
{
	bool can_be_true;
	bool can_be_false;
	
	BoolMask() {}
	BoolMask(bool can_be_true_, bool can_be_false_) : can_be_true(can_be_true_), can_be_false(can_be_false_) {}
	
	BoolMask operator &(const BoolMask & m)
	{
		return BoolMask(can_be_true && m.can_be_true, can_be_false || m.can_be_false);
	}
	BoolMask operator |(const BoolMask & m)
	{
		return BoolMask(can_be_true || m.can_be_true, can_be_false && m.can_be_false);
	}
	BoolMask operator !()
	{
		return BoolMask(can_be_false, can_be_true);
	}
};

bool PKCondition::mayBeTrueInRange(const Field * left_pk, const Field * right_pk, bool right_bounded)
{
	/// Найдем диапазоны элементов ключа.
	std::vector<Range> key_ranges(sort_descr.size(), Range());
	
	if (right_bounded)
	{
		for (size_t i = 0; i < sort_descr.size(); ++i)
		{
			if (left_pk[i] == right_pk[i])
			{
				key_ranges[i] = Range(left_pk[i]);
			}
			else
			{
				key_ranges[i] = Range(left_pk[i], true, right_pk[i], true);
				break;
			}
		}
	}
	else
	{
		key_ranges[0] = Range::createLeftBounded(left_pk[0], true);
	}

	std::vector<BoolMask> rpn_stack;
	for (size_t i = 0; i < rpn.size(); ++i)
	{
		RPNElement & element = rpn[i];
		if (element.function == RPNElement::FUNCTION_UNKNOWN)
		{
			rpn_stack.push_back(BoolMask(true, true));
		}
		else if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE || element.function == RPNElement::FUNCTION_IN_RANGE)
		{
			const Range & key_range = key_ranges[element.key_column];
			bool intersects = element.range.intersectsRange(key_range);
			bool contains = element.range.containsRange(key_range);

			rpn_stack.push_back(BoolMask(intersects, !contains));
			if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
				rpn_stack.back() = !rpn_stack.back();
		}
		else if (element.function == RPNElement::FUNCTION_NOT)
		{
			rpn_stack.back() = !rpn_stack.back();
		}
		else if (element.function == RPNElement::FUNCTION_AND)
		{
			BoolMask arg1 = rpn_stack.back();
			rpn_stack.pop_back();
			BoolMask arg2 = rpn_stack.back();
			rpn_stack.back() = arg1 & arg2;
		}
		else if (element.function == RPNElement::FUNCTION_OR)
		{
			BoolMask arg1 = rpn_stack.back();
			rpn_stack.pop_back();
			BoolMask arg2 = rpn_stack.back();
			rpn_stack.back() = arg1 | arg2;
		}
		else
			throw Exception("Unexpected function type in PKCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
	}
	
	if (rpn_stack.size() != 1)
		throw Exception("Unexpected stack size in PkCondition::mayBeTrueInRange", ErrorCodes::LOGICAL_ERROR);
	
	return rpn_stack[0].can_be_true;
}

bool PKCondition::mayBeTrueInRange(const Field * left_pk, const Field * right_pk)
{
	return mayBeTrueInRange(left_pk, right_pk, true);
}

bool PKCondition::mayBeTrueAfter(const Field * left_pk)
{
	return mayBeTrueInRange(left_pk, NULL, false);
}

}
