#include <Storages/IStorage.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/formatAST.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateQuery.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

BlockIO InterpreterShowCreateQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterShowCreateQuery::getSampleBlock()
{
    return Block{{
        ColumnString::create(),
        std::make_shared<DataTypeString>(),
        "statement"}};
}


BlockInputStreamPtr InterpreterShowCreateQuery::executeImpl()
{
    const ASTShowCreateQuery & ast = typeid_cast<const ASTShowCreateQuery &>(*query_ptr);

    if (ast.temporary && !ast.database.empty())
        throw Exception("Temporary databases are not possible.", ErrorCodes::SYNTAX_ERROR);

    ASTPtr create_query = (ast.temporary ? context.getCreateExternalQuery(ast.table) :
                          context.getCreateQuery(ast.database, ast.table));

    if (!create_query && ast.temporary)
        throw Exception("Unable to show the create query of " + ast.table + ". Maybe it is created by the system.");

    std::stringstream stream;
    formatAST(*create_query, stream, false, true);
    String res = stream.str();

    MutableColumnPtr column = ColumnString::create();
    column->insert(res);

    return std::make_shared<OneBlockInputStream>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}});
}

}
