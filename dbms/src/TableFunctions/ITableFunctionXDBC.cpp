#include <TableFunctions/TableFunctionODBC.h>
#include <type_traits>
#include <ext/scope_guard.h>

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Poco/Net/HTTPRequest.h>
#include <Storages/StorageODBC.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/ITableFunctionXDBC.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/ODBCBridgeHelper.h>
#include <Core/Defines.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    }

    StoragePtr ITableFunctionXDBC::executeImpl(const ASTPtr & ast_function, const Context & context) const
    {
        const ASTFunction & args_func = typeid_cast<const ASTFunction &>(*ast_function);

        if (!args_func.arguments)
            throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

        ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.arguments).children;
        if (args.size() != 2 && args.size() != 3)
            throw Exception("Table function '" + getName() + "' requires 2 or 3 arguments: ODBC('DSN', table) or ODBC('DSN', schema, table)",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto i = 0u; i < args.size(); ++i)
            args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

        std::string connection_string = "";
        std::string schema_name = "";
        std::string table_name = "";
        if (args.size() == 3)
        {
            connection_string = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
            schema_name = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();
            table_name = static_cast<const ASTLiteral &>(*args[2]).value.safeGet<String>();
        } else if (args.size() == 2)
        {
            connection_string = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
            table_name = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();
        }

        const auto & config = context.getConfigRef();

        /* Infer external table structure */
        BridgeHelperPtr helper = createBridgeHelper(config, context.getSettingsRef().http_receive_timeout.value, connection_string);
        helper->startBridgeSync();

        Poco::URI columns_info_uri = helper->getColumnsInfoURI();
        columns_info_uri.addQueryParameter("connection_string", connection_string);
        if (!schema_name.empty())
            columns_info_uri.addQueryParameter("schema", schema_name);
        columns_info_uri.addQueryParameter("table", table_name);

        ReadWriteBufferFromHTTP buf(columns_info_uri, Poco::Net::HTTPRequest::HTTP_POST, nullptr);

        std::string columns_info;
        readStringBinary(columns_info, buf);
        NamesAndTypesList columns = NamesAndTypesList::parse(columns_info);

        auto result = createStorage(table_name, connection_string, schema_name, table_name, ColumnsDescription{columns}, context);

        if(!result)
            throw Exception("Failed to instantiate storage from table function " + getName())

        result->startup();
        return result;
    }

}
