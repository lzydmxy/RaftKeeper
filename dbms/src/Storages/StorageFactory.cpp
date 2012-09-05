#include <Poco/Util/Application.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Interpreters/Context.h>

#include <DB/Storages/StorageLog.h>
#include <DB/Storages/StorageTinyLog.h>
#include <DB/Storages/StorageMemory.h>
#include <DB/Storages/StorageMerge.h>
#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/StorageDistributed.h>
#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/Storages/StorageSystemOne.h>
#include <DB/Storages/StorageFactory.h>


namespace DB
{


StoragePtr StorageFactory::get(
	const String & name,
	const String & data_path,
	const String & table_name,
	Context & context,
	ASTPtr & query,
	NamesAndTypesListPtr columns) const
{
	if (name == "Log")
	{
		return new StorageLog(data_path, table_name, columns);
	}
	else if (name == "TinyLog")
	{
		return new StorageTinyLog(data_path, table_name, columns);
	}
	else if (name == "Memory")
	{
		return new StorageMemory(table_name, columns);
	}
	else if (name == "Merge")
	{
		/** В запросе в качестве аргумента для движка указано имя БД, в которой находятся таблицы-источники,
		  *  а также регексп для имён таблиц-источников.
		  */
		ASTs & args_func = dynamic_cast<ASTFunction &>(*dynamic_cast<ASTCreateQuery &>(*query).storage).children;

		if (args_func.size() != 1)
			throw Exception("Storage Merge requires exactly 2 parameters"
				" - name of source database and regexp for table names.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		ASTs & args = dynamic_cast<ASTExpressionList &>(*args_func.at(0)).children;

		if (args.size() != 2)
			throw Exception("Storage Merge requires exactly 2 parameters"
				" - name of source database and regexp for table names.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String source_database 		= dynamic_cast<ASTIdentifier &>(*args[0]).name;
		String table_name_regexp	= boost::get<const String &>(dynamic_cast<ASTLiteral &>(*args[1]).value);
		
		return new StorageMerge(table_name, columns, source_database, table_name_regexp, context);
	}
	else if (name == "Distributed")
	{
		/** В запросе в качестве аргумента для движка указано имя конфигурационной секции,
		  *  в которой задан список удалённых серверов, а также имя удалённой БД и имя удалённой таблицы.
		  */
		ASTs & args_func = dynamic_cast<ASTFunction &>(*dynamic_cast<ASTCreateQuery &>(*query).storage).children;

		if (args_func.size() != 1)
			throw Exception("Storage Distributed requires exactly 3 parameters"
				" - name of configuration section with list of remote servers, name of remote database and name of remote table.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		ASTs & args = dynamic_cast<ASTExpressionList &>(*args_func.at(0)).children;
		
		if (args.size() != 3)
			throw Exception("Storage Distributed requires exactly 3 parameters"
				" - name of configuration section with list of remote servers, name of remote database and name of remote table.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		String config_name 		= dynamic_cast<ASTIdentifier &>(*args[0]).name;
		String remote_database 	= dynamic_cast<ASTIdentifier &>(*args[1]).name;
		String remote_table 	= dynamic_cast<ASTIdentifier &>(*args[2]).name;
		
		StorageDistributed::Addresses addresses;

		Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();
		Poco::Util::AbstractConfiguration::Keys config_keys;
		config.keys("remote_servers." + config_name, config_keys);

		String config_prefix = "remote_servers." + config_name + ".";
			
		for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
			addresses.push_back(Poco::Net::SocketAddress(
				config.getString(config_prefix + *it + ".host"),
				config.getInt(config_prefix + *it + ".port")));
		
		return new StorageDistributed(table_name, columns, addresses, remote_database, remote_table,
			context.getDataTypeFactory(), context.getSettings());
	}
	else if (name == "MergeTree")
	{
		/** В качестве аргумента для движка должно быть указано:
		  *  - имя столбца с датой;
		  *  - выражение для сортировки в скобках;
		  *  - index_granularity.
		  * Например: ENGINE = MergeTree(EventDate, (CounterID, EventDate, intHash32(UniqID), EventTime), 8192).
		  */
		ASTs & args_func = dynamic_cast<ASTFunction &>(*dynamic_cast<ASTCreateQuery &>(*query).storage).children;

		if (args_func.size() != 1)
			throw Exception("Storage MergeTree requires exactly 3 parameters"
				" - name of column with date, primary key expression, index granularity.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		ASTs & args = dynamic_cast<ASTExpressionList &>(*args_func.at(0)).children;

		if (args.size() != 3)
			throw Exception("Storage MergeTree requires exactly 3 parameters"
				" - name of column with date, primary key expression, index granularity.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String date_column_name 	= dynamic_cast<ASTIdentifier &>(*args[0]).name;
		UInt64 index_granularity	= boost::get<UInt64>(dynamic_cast<ASTLiteral &>(*args[2]).value);
		ASTFunction & primary_expr_func = dynamic_cast<ASTFunction &>(*args[1]);
		
		if (primary_expr_func.name != "tuple")
			throw Exception("Primary expression for storage MergeTree must be in parentheses.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		ASTPtr primary_expr = primary_expr_func.children.at(0);

		return new StorageMergeTree(data_path, table_name, columns, context, primary_expr, date_column_name, index_granularity);
	}
	else if (name == "CollapsingMergeTree")
	{
		/** В качестве аргумента для движка должно быть указано:
		  *  - имя столбца с датой;
		  *  - выражение для сортировки в скобках;
		  *  - index_granularity;
		  *  - имя столбца, содержащего тип строчки с изменением "визита" (принимающего значения 1 и -1).
		  * Например: ENGINE = CollapsingMergeTree(EventDate, (CounterID, EventDate, intHash32(UniqID), VisitID), 8192, Sign).
		  */
		ASTs & args_func = dynamic_cast<ASTFunction &>(*dynamic_cast<ASTCreateQuery &>(*query).storage).children;

		if (args_func.size() != 1)
			throw Exception("Storage CollapsingMergeTree requires exactly 4 parameters"
				" - name of column with date, primary key expression, index granularity, sign_column.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		ASTs & args = dynamic_cast<ASTExpressionList &>(*args_func.at(0)).children;

		if (args.size() != 4)
			throw Exception("Storage CollapsingMergeTree requires exactly 4 parameters"
				" - name of column with date, primary key expression, index granularity, sign_column.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String date_column_name 	= dynamic_cast<ASTIdentifier &>(*args[0]).name;
		UInt64 index_granularity	= boost::get<UInt64>(dynamic_cast<ASTLiteral &>(*args[2]).value);
		String sign_column_name 	= dynamic_cast<ASTIdentifier &>(*args[3]).name;
		ASTFunction & primary_expr_func = dynamic_cast<ASTFunction &>(*args[1]);

		if (primary_expr_func.name != "tuple")
			throw Exception("Primary expression for storage CollapsingMergeTree must be in parentheses.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		ASTPtr primary_expr = primary_expr_func.children.at(0);

		return new StorageMergeTree(data_path, table_name, columns, context, primary_expr, date_column_name, index_granularity, sign_column_name);
	}
	else if (name == "SystemNumbers")
	{
		if (columns->size() != 1 || columns->begin()->first != "number" || columns->begin()->second->getName() != "UInt64")
			throw Exception("Storage SystemNumbers only allows one column with name 'number' and type 'UInt64'",
				ErrorCodes::ILLEGAL_COLUMN);

		return new StorageSystemNumbers(table_name);
	}
	else if (name == "SystemOne")
	{
		if (columns->size() != 1 || columns->begin()->first != "dummy" || columns->begin()->second->getName() != "UInt8")
			throw Exception("Storage SystemOne only allows one column with name 'dummy' and type 'UInt8'",
				ErrorCodes::ILLEGAL_COLUMN);

		return new StorageSystemOne(table_name);
	}
	else
		throw Exception("Unknown storage " + name, ErrorCodes::UNKNOWN_STORAGE);
}


}
