#pragma once

#include <Interpreters/QueryParameterVisitor.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>


namespace DB
{

QueryParameterVisitor::QueryParameterVisitor(NameSet & parameters_name)
    : query_parameters(parameters_name)
{
}

void QueryParameterVisitor::visit(const ASTPtr & ast)
{
    for (const auto & child : ast->children)
    {
        if (const auto & query_parameter = child->as<ASTQueryParameter>())
            visitQueryParameter(*query_parameter);
        else
            visit(child);
    }
}

void QueryParameterVisitor::visitQueryParameter(const ASTQueryParameter & query_parameter)
{
    query_parameters.insert(query_parameter.name);
}

NameSet analyzeReceiveQueryParams(const std::string & query)
{
    NameSet query_params;
    const char * query_begin = query.data();
    const char * query_end = query.data() + query.size();

    ParserQuery parser(query_end, false);
    ASTPtr extract_query_ast = parseQuery(parser, query_begin, query_end, "analyzeReceiveQueryParams", 0, 0);
    QueryParameterVisitor(query_params).visit(extract_query_ast);
    return query_params;
}

}

