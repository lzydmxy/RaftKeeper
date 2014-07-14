#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Parsers/ASTAlterQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Parsers/ASTIdentifier.h>

#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/IO/copyData.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Storages/StorageMerge.h>
#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>

#include <Poco/FileStream.h>

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/bind/placeholders.hpp>


using namespace DB;

InterpreterAlterQuery::InterpreterAlterQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
}

void InterpreterAlterQuery::execute()
{
	ASTAlterQuery & alter = typeid_cast<ASTAlterQuery &>(*query_ptr);
	String & table_name = alter.table;
	String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;
	AlterCommands commands = parseAlter(alter.parameters, context.getDataTypeFactory());

	StoragePtr table = context.getTable(database_name, table_name);
	table->alter(commands, database_name, table_name, context);
}

AlterCommands InterpreterAlterQuery::parseAlter(
	const ASTAlterQuery::ParameterContainer & params_container, const DataTypeFactory & data_type_factory)
{
	AlterCommands res;

	for (const auto & params : params_container)
	{
		res.push_back(AlterCommand());
		AlterCommand & command = res.back();

		if (params.type == ASTAlterQuery::ADD)
		{
			command.type = AlterCommand::ADD;

			const ASTNameTypePair & ast_name_type = typeid_cast<const ASTNameTypePair &>(*params.name_type);
			StringRange type_range = ast_name_type.type->range;
			String type_string = String(type_range.first, type_range.second - type_range.first);

			command.column_name = ast_name_type.name;
			command.data_type = data_type_factory.get(type_string);

			if (params.column)
				command.after_column = typeid_cast<const ASTIdentifier &>(*params.column).name;
		}
		else if (params.type == ASTAlterQuery::DROP)
		{
			command.type = AlterCommand::DROP;
			command.column_name = typeid_cast<const ASTIdentifier &>(*(params.column)).name;
		}
		else if (params.type == ASTAlterQuery::MODIFY)
		{
			command.type = AlterCommand::MODIFY;

			const ASTNameTypePair & ast_name_type = typeid_cast<const ASTNameTypePair &>(*params.name_type);
			StringRange type_range = ast_name_type.type->range;
			String type_string = String(type_range.first, type_range.second - type_range.first);

			command.column_name = ast_name_type.name;
			command.data_type = data_type_factory.get(type_string);
		}
		else
			throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
	}

	return res;
}

void InterpreterAlterQuery::updateMetadata(
	const String & database_name, const String & table_name, const NamesAndTypesList & columns, Context & context)
{
	String path = context.getPath();

	String database_name_escaped = escapeForFileName(database_name);
	String table_name_escaped = escapeForFileName(table_name);

	String metadata_path = path + "metadata/" + database_name_escaped + "/" + table_name_escaped + ".sql";
	String metadata_temp_path = metadata_path + ".tmp";

	StringPtr query = new String();
	{
		ReadBufferFromFile in(metadata_path);
		WriteBufferFromString out(*query);
		copyData(in, out);
	}

	const char * begin = query->data();
	const char * end = begin + query->size();
	const char * pos = begin;

	ParserCreateQuery parser;
	ASTPtr ast;
	Expected expected = "";
	bool parse_res = parser.parse(pos, end, ast, expected);

	/// Распарсенный запрос должен заканчиваться на конец входных данных или на точку с запятой.
	if (!parse_res || (pos != end && *pos != ';'))
		throw Exception(getSyntaxErrorMessage(parse_res, begin, end, pos, expected, "in file " + metadata_path),
			DB::ErrorCodes::SYNTAX_ERROR);

	ast->query_string = query;

	ASTCreateQuery & attach = typeid_cast<ASTCreateQuery &>(*ast);

	ASTPtr new_columns = InterpreterCreateQuery::formatColumns(columns);
	*std::find(attach.children.begin(), attach.children.end(), attach.columns) = new_columns;
	attach.columns = new_columns;

	{
		Poco::FileOutputStream ostr(metadata_temp_path);
		formatAST(attach, ostr, 0, false);
	}

	Poco::File(metadata_temp_path).renameTo(metadata_path);
}
