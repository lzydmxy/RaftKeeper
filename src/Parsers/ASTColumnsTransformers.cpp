#include <map>
#include "ASTColumnsTransformers.h"
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

void IASTColumnsTransformer::transform(const ASTPtr & transformer, ASTs & nodes)
{
    if (const auto * apply = transformer->as<ASTColumnsApplyTransformer>())
    {
        apply->transform(nodes);
    }
    else if (const auto * except = transformer->as<ASTColumnsExceptTransformer>())
    {
        except->transform(nodes);
    }
    else if (const auto * replace = transformer->as<ASTColumnsReplaceTransformer>())
    {
        replace->transform(nodes);
    }
}

void ASTColumnsApplyTransformer::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "APPLY" << (settings.hilite ? hilite_none : "") << "(" << func_name << ")";
}

void ASTColumnsApplyTransformer::transform(ASTs & nodes) const
{
    for (auto & column : nodes)
    {
        column = makeASTFunction(func_name, column);
    }
}

void ASTColumnsExceptTransformer::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
    settings.ostr << (is_strict ? "EXCEPTSTRICT" : "EXCEPT");
    settings.ostr << (settings.hilite ? hilite_none : "") << "(";

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            settings.ostr << ", ";
        }
        (*it)->formatImpl(settings, state, frame);
    }

    settings.ostr << ")";
}

void ASTColumnsExceptTransformer::transform(ASTs & nodes) const
{
    ASTs unexcepted_columns(children);
    nodes.erase(
        std::remove_if(
            nodes.begin(),
            nodes.end(),
            [&](const ASTPtr & node_child)
            {
                if (const auto * id = node_child->as<ASTIdentifier>())
                {
                    for (size_t i = 0; i < children.size(); i++)
                    {
                        if (children[i]->as<const ASTIdentifier &>().name == id->shortName())
                        {
                            unexcepted_columns.erase(unexcepted_columns.begin() + i);
                            return true;
                        }
                    }
                }
                return false;
            }),
        nodes.end());

    if (is_strict && !unexcepted_columns.empty())
    {
        String unexisted_columns;
        for (size_t i = 0; i < unexcepted_columns.size(); ++i)
        {
            if (i > 0)
                unexisted_columns += ", ";
            unexisted_columns += unexcepted_columns[i]->as<const ASTIdentifier &>().name;
        }

        throw Exception(
            "Columns transformer EXPCEPTSTRICT process unexist column : " + unexisted_columns,
            ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    }
}

void ASTColumnsReplaceTransformer::Replacement::formatImpl(
    const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    expr->formatImpl(settings, state, frame);
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "") << name;
}

void ASTColumnsReplaceTransformer::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
    settings.ostr << (is_strict ? "REPLACESTRICT" : "REPLACE");
    settings.ostr << (settings.hilite ? hilite_none : "") << "(";

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            settings.ostr << ", ";
        }
        (*it)->formatImpl(settings, state, frame);
    }

    settings.ostr << ")";
}

void ASTColumnsReplaceTransformer::replaceChildren(ASTPtr & node, const ASTPtr & replacement, const String & name)
{
    for (auto & child : node->children)
    {
        if (const auto * id = child->as<ASTIdentifier>())
        {
            if (id->shortName() == name)
                child = replacement->clone();
        }
        else
            replaceChildren(child, replacement, name);
    }
}

void ASTColumnsReplaceTransformer::transform(ASTs & nodes) const
{
    std::map<String, ASTPtr> replace_map;
    for (const auto & replace_child : children)
    {
        auto & replacement = replace_child->as<Replacement &>();
        if (replace_map.find(replacement.name) != replace_map.end())
            throw Exception(
                "Expressions in columns transformer REPLACE should not contain the same replacement more than once",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        replace_map.emplace(replacement.name, replacement.expr);
    }

    for (auto & column : nodes)
    {
        if (const auto * id = column->as<ASTIdentifier>())
        {
            auto replace_it = replace_map.find(id->shortName());
            if (replace_it != replace_map.end())
            {
                column = replace_it->second;
                column->setAlias(replace_it->first);
                replace_map.erase(replace_it);
            }
        }
        else if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(column.get()))
        {
            auto replace_it = replace_map.find(ast_with_alias->alias);
            if (replace_it != replace_map.end())
            {
                auto new_ast = replace_it->second->clone();
                ast_with_alias->alias = ""; // remove the old alias as it's useless after replace transformation
                replaceChildren(new_ast, column, replace_it->first);
                column = new_ast;
                column->setAlias(replace_it->first);
                replace_map.erase(replace_it);
            }
        }
    }

    if (is_strict && !replace_map.empty())
    {
        String unexisted_columns = "";
        for (auto it = replace_map.begin(); it != replace_map.end(); ++it)
        {
            if (unexisted_columns != "")
                unexisted_columns += ", ";
            unexisted_columns += it->first;
        }
        throw Exception(
            "Columns transformer REPLACESTRICT process unexist column : " + unexisted_columns,
            ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    }

}

}
