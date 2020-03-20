#include <Common/typeid_cast.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/JoinToSubqueryTransformVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/AsteriskSemantic.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_IDENTIFIER;
}

namespace
{

/// @note we use `--` prefix for unique short names and `--.` for subqueries.
/// It expects that user do not use names starting with `--` and column names starting with dot.
ASTPtr makeSubqueryTemplate()
{
    ParserTablesInSelectQueryElement parser(true);
    ASTPtr subquery_template = parseQuery(parser, "(select * from _t) as `--.s`", 0);
    if (!subquery_template)
        throw Exception("Cannot parse subquery template", ErrorCodes::LOGICAL_ERROR);
    return subquery_template;
}

ASTPtr makeSubqueryQualifiedAsterisk()
{
    auto asterisk = std::make_shared<ASTQualifiedAsterisk>();
    asterisk->children.emplace_back(std::make_shared<ASTIdentifier>("--.s"));
    return asterisk;
}

/// Replace asterisks in select_expression_list with column identifiers
class ExtractAsterisksMatcher
{
public:
    struct Data
    {
        std::unordered_map<String, NamesAndTypesList> table_columns;
        std::vector<String> tables_order;
        std::shared_ptr<ASTExpressionList> new_select_expression_list;

        Data(const std::vector<TableWithColumnNamesAndTypes> & tables)
        {
            tables_order.reserve(tables.size());
            for (const auto & table : tables)
            {
                String table_name = table.table.getQualifiedNamePrefix(false);
                NamesAndTypesList columns = table.columns;
                tables_order.push_back(table_name);
                table_columns.emplace(std::move(table_name), std::move(columns));
            }
        }

        void addTableColumns(const String & table_name)
        {
            auto it = table_columns.find(table_name);
            if (it == table_columns.end())
                throw Exception("Unknown qualified identifier: " + table_name, ErrorCodes::UNKNOWN_IDENTIFIER);

            for (const auto & column : it->second)
                new_select_expression_list->children.push_back(
                    std::make_shared<ASTIdentifier>(std::vector<String>{it->first, column.name}));
        }
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return false; }

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (auto * t = ast->as<ASTExpressionList>())
            visit(*t, ast, data);
    }

private:
    static void visit(const ASTExpressionList & node, const ASTPtr &, Data & data)
    {
        bool has_asterisks = false;
        data.new_select_expression_list = std::make_shared<ASTExpressionList>();
        data.new_select_expression_list->children.reserve(node.children.size());

        for (auto & child : node.children)
        {
            if (child->as<ASTAsterisk>())
            {
                has_asterisks = true;

                for (auto & table_name : data.tables_order)
                    data.addTableColumns(table_name);
            }
            else if (child->as<ASTQualifiedAsterisk>())
            {
                has_asterisks = true;

                if (child->children.size() != 1)
                    throw Exception("Logical error: qualified asterisk must have exactly one child", ErrorCodes::LOGICAL_ERROR);
                ASTIdentifier & identifier = child->children[0]->as<ASTIdentifier &>();

                data.addTableColumns(identifier.name);
            }
            else
                data.new_select_expression_list->children.push_back(child);
        }

        if (!has_asterisks)
            data.new_select_expression_list.reset();
    }
};

/// Find columns with aliases to push them into rewritten subselects.
/// Normalize table aliases: table_name.column_name -> table_alias.column_name
/// Make aliases maps (alias -> column_name, column_name -> alias)
struct ColumnAliasesMatcher
{
    struct Data
    {
        const std::vector<DatabaseAndTableWithAlias> tables;
        bool public_names;
        AsteriskSemantic::RevertedAliases rev_aliases;  /// long_name -> aliases
        std::unordered_map<String, String> aliases;     /// alias -> long_name
        std::vector<std::pair<ASTIdentifier *, bool>> compound_identifiers;
        std::set<String> allowed_long_names;            /// original names allowed as aliases '--t.x as t.x' (select expressions only).

        explicit Data(const std::vector<DatabaseAndTableWithAlias> && tables_)
            : tables(tables_)
            , public_names(false)
        {}

        void replaceIdentifiersWithAliases()
        {
            String hide_prefix = "--"; /// @note restriction: user should not use alises like `--table.column`

            for (auto & [identifier, is_public] : compound_identifiers)
            {
                String long_name = identifier->name;

                auto it = rev_aliases.find(long_name);
                if (it == rev_aliases.end())
                {
                    bool last_table = false;
                    {
                        if (auto best_table_pos = IdentifierSemantic::chooseTable(*identifier, tables))
                            last_table = (*best_table_pos + 1 == tables.size());
                    }

                    if (!last_table)
                    {
                        String alias = hide_prefix + long_name;
                        aliases[alias] = long_name;
                        rev_aliases[long_name].push_back(alias);

                        IdentifierSemantic::coverName(*identifier, alias);
                        if (is_public)
                        {
                            identifier->setAlias(long_name);
                            allowed_long_names.insert(long_name);
                        }
                    }
                    else if (is_public)
                        identifier->setAlias(long_name); /// prevent crop long to short name
                }
                else
                {
                    if (it->second.empty())
                        throw Exception("No alias for '" + long_name + "'", ErrorCodes::LOGICAL_ERROR);

                    if (is_public && allowed_long_names.count(long_name))
                        ; /// leave original name unchanged for correct output
                    else
                        IdentifierSemantic::coverName(*identifier, it->second[0]);
                }
            }
        }
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        return !node->as<ASTQualifiedAsterisk>();
    }

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (auto * t = ast->as<ASTIdentifier>())
            visit(*t, ast, data);

        if (ast->as<ASTAsterisk>() || ast->as<ASTQualifiedAsterisk>())
            throw Exception("Multiple JOIN do not support asterisks for complex queries yet", ErrorCodes::NOT_IMPLEMENTED);
    }

    static void visit(const ASTIdentifier & const_node, const ASTPtr &, Data & data)
    {
        ASTIdentifier & node = const_cast<ASTIdentifier &>(const_node); /// we know it's not const
        if (node.isShort())
            return;

        bool last_table = false;
        String long_name;

        if (auto table_pos = IdentifierSemantic::chooseTable(node, data.tables))
        {
            auto & table = data.tables[*table_pos];
            IdentifierSemantic::setColumnLongName(node, table); /// table_name.column_name -> table_alias.column_name
            long_name = node.name;
            if (&table == &data.tables.back())
                last_table = true;
        }

        if (long_name.empty())
            throw Exception("Cannot refer column '" + node.name + "' to table", ErrorCodes::AMBIGUOUS_COLUMN_NAME);

        String alias = node.tryGetAlias();
        if (!alias.empty())
        {
            data.aliases[alias] = long_name;
            data.rev_aliases[long_name].push_back(alias);

            if (!last_table)
            {
                IdentifierSemantic::coverName(node, alias);
                node.setAlias("");
            }
        }
        else if (node.compound())
            data.compound_identifiers.emplace_back(&node, data.public_names);
    }
};

/// Attach additional semantic info to generated selects.
struct AppendSemanticVisitorData
{
    using TypeToVisit = ASTSelectQuery;

    AsteriskSemantic::RevertedAliasesPtr rev_aliases = {};
    bool done = false;

    void visit(ASTSelectQuery & select, ASTPtr &)
    {
        if (done || !rev_aliases || !select.select())
            return;

        for (auto & child : select.select()->children)
        {
            if (auto * node = child->as<ASTAsterisk>())
                AsteriskSemantic::setAliases(*node, rev_aliases);
            if (auto * node = child->as<ASTQualifiedAsterisk>())
                AsteriskSemantic::setAliases(*node, rev_aliases);
        }

        done = true;
    }
};

/// Replaces table elements with pair.
struct RewriteTablesVisitorData
{
    using TypeToVisit = ASTTablesInSelectQuery;

    ASTPtr left;
    ASTPtr right;
    bool done = false;

    /// @note Do not change ASTTablesInSelectQuery itself. No need to change select.tables.
    void visit(ASTTablesInSelectQuery &, ASTPtr & ast)
    {
        if (done)
            return;
        std::vector<ASTPtr> new_tables{left, right};
        ast->children.swap(new_tables);
        done = true;
    }
};

template <size_t version = 1>
bool needRewrite(ASTSelectQuery & select, std::vector<const ASTTableExpression *> & table_expressions)
{
    if (!select.tables())
        return false;

    const auto * tables = select.tables()->as<ASTTablesInSelectQuery>();
    if (!tables)
        return false;

    size_t num_tables = tables->children.size();
    if (num_tables <= 2)
        return false;

    size_t num_array_join = 0;
    size_t num_using = 0;

    table_expressions.reserve(num_tables);
    for (size_t i = 0; i < num_tables; ++i)
    {
        const auto * table = tables->children[i]->as<ASTTablesInSelectQueryElement>();
        if (!table)
            throw Exception("Table expected", ErrorCodes::LOGICAL_ERROR);

        if (table->table_expression)
            if (const auto * expression = table->table_expression->as<ASTTableExpression>())
                table_expressions.push_back(expression);
        if (!i)
            continue;

        if (!table->table_join && !table->array_join)
            throw Exception("Joined table expected", ErrorCodes::LOGICAL_ERROR);

        if (table->array_join)
        {
            ++num_array_join;
            continue;
        }

        const auto & join = table->table_join->as<ASTTableJoin &>();
        if (isComma(join.kind))
            throw Exception("COMMA to CROSS JOIN rewriter is not enabled or cannot rewrite query", ErrorCodes::NOT_IMPLEMENTED);

        if (join.using_expression_list)
            ++num_using;
    }

    if (num_tables - num_array_join <= 2)
        return false;

    /// it's not trivial to support mix of JOIN ON & JOIN USING cause of short names
    if (num_using && version <= 1)
        throw Exception("Multiple JOIN does not support USING", ErrorCodes::NOT_IMPLEMENTED);
    if (num_array_join)
        throw Exception("Multiple JOIN does not support mix with ARRAY JOINs", ErrorCodes::NOT_IMPLEMENTED);
    return true;
}

using RewriteMatcher = OneTypeMatcher<RewriteTablesVisitorData>;
using RewriteVisitor = InDepthNodeVisitor<RewriteMatcher, true>;
using ExtractAsterisksVisitor = ConstInDepthNodeVisitor<ExtractAsterisksMatcher, true>;
using ColumnAliasesVisitor = ConstInDepthNodeVisitor<ColumnAliasesMatcher, true>;
using AppendSemanticMatcher = OneTypeMatcher<AppendSemanticVisitorData>;
using AppendSemanticVisitor = InDepthNodeVisitor<AppendSemanticMatcher, true>;

/// V2 specific visitors

struct CollectColumnIdentifiersMatcher
{
    using Data = std::vector<ASTIdentifier *>;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        /// Do not go into subqueries. Do not collect table identifiers. Do not get identifier from 't.*'.
        return !node->as<ASTSubquery>() &&
            !node->as<ASTTablesInSelectQuery>() &&
            !node->as<ASTQualifiedAsterisk>();
    }

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (auto * t = ast->as<ASTIdentifier>())
            visit(*t, ast, data);
    }

    static void visit(const ASTIdentifier & ident, const ASTPtr &, Data & data)
    {
        data.push_back(const_cast<ASTIdentifier *>(&ident));
    }
};
using CollectColumnIdentifiersVisitor = ConstInDepthNodeVisitor<CollectColumnIdentifiersMatcher, true>;

struct CheckAliasDependencyVisitorData
{
    using TypeToVisit = ASTIdentifier;

    const Aliases & aliases;
    const ASTIdentifier * dependency = nullptr;

    void visit(ASTIdentifier & ident, ASTPtr &)
    {
        if (!dependency && aliases.count(ident.name))
            dependency = &ident;
    }
};
using CheckAliasDependencyMatcher = OneTypeMatcher<CheckAliasDependencyVisitorData>;
using CheckAliasDependencyVisitor = InDepthNodeVisitor<CheckAliasDependencyMatcher, true>;

struct RewriteWithAliasMatcher
{
    using Data = std::unordered_map<String, ASTPtr>;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr &)
    {
        return !node->as<ASTSubquery>();
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        String alias = ast->tryGetAlias();
        if (!alias.empty())
        {
            auto it = data.find(alias);
            if (it != data.end() && it->second.get() == ast.get())
                ast = std::make_shared<ASTIdentifier>(alias);
        }
    }
};
using RewriteWithAliasVisitor = InDepthNodeVisitor<RewriteWithAliasMatcher, true>;

class SubqueryExpressionsRewriteMatcher
{
public:
    struct Data
    {
        ASTPtr expression_list;
        bool done = false;
    };

    static bool needChildVisit(ASTPtr & node, ASTPtr &)
    {
        return !node->as<ASTSelectQuery>();
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = ast->as<ASTSelectQuery>())
            visit(*t, ast, data);
    }

private:
    static void visit(ASTSelectQuery & select, ASTPtr &, Data & data)
    {
        if (!data.done)
            select.setExpression(ASTSelectQuery::Expression::SELECT, std::move(data.expression_list));
        data.done = true;
    }
};
using SubqueryExpressionsRewriteVisitor = InDepthNodeVisitor<SubqueryExpressionsRewriteMatcher, true>;

struct TableNeededColumns
{
    const DatabaseAndTableWithAlias & table;
    NameSet no_clashes = {};
    NameSet alias_clashes = {};
    std::unordered_map<String, String> column_clashes = {};

    void fillExpressionList(ASTExpressionList & expression_list) const
    {
        size_t columns_count = no_clashes.size() + column_clashes.size() + alias_clashes.size();
        expression_list.children.reserve(expression_list.children.size() + columns_count);

        String table_name = table.getQualifiedNamePrefix(false);

        for (auto & column : no_clashes)
            addShortName(column, expression_list);

        for (auto & column : alias_clashes)
            addShortName(column, expression_list);

        for (auto & [column, alias] : column_clashes)
            addAliasedName(table_name, column, alias, expression_list);
    }

    static void addShortName(const String & column, ASTExpressionList & expression_list)
    {
        auto ident = std::make_shared<ASTIdentifier>(column);
        expression_list.children.emplace_back(std::move(ident));
    }

    /// t.x as `some`
    static void addAliasedName(const String & table, const String & column, const String & alias, ASTExpressionList & expression_list)
    {
        auto ident = std::make_shared<ASTIdentifier>(std::vector<String>{table, column});
        ident->setAlias(alias);
        expression_list.children.emplace_back(std::move(ident));
    }
};

class UniqueShortNames
{
public:
    /// We know that long names are unique (do not clashes with others).
    /// So we could make unique names base on this knolage by adding some unused prefix.
    static constexpr const char * pattern = "--";

    String longToShort(const String & long_name)
    {
        auto it = long_to_short.find(long_name);
        if (it != long_to_short.end())
            return it->second;

        String short_name = generateUniqueName(long_name);
        long_to_short.emplace(long_name, short_name);
        return short_name;
    }

private:
    std::unordered_map<String, String> long_to_short;

    String generateUniqueName(const String & long_name)
    {
        return String(pattern) + long_name;
    }
};

size_t countTablesWithColumn(const std::vector<TableWithColumnNamesAndTypes> & tables, const String & short_name)
{
    size_t count = 0;
    for (auto & table : tables)
        if (table.hasColumn(short_name))
            ++count;
    return count;
}

/// 'select `--t.x`, `--t.x`, ...' -> 'select `--t.x` as `t.x`, `t.x`, ...'
void restoreName(ASTIdentifier & ident, const String & original_name, NameSet & restored_names)
{
    if (!ident.tryGetAlias().empty())
        return;
    if (original_name.empty())
        return;

    if (!restored_names.count(original_name))
    {
        ident.setAlias(original_name);
        restored_names.emplace(original_name);
    }
    else
        ident.setShortName(original_name);
}

/// Find clashes and normalize names
/// 1. If column name has no clashes make all its occurrences short: 'table.column' -> 'column', 'table_alias.column' -> 'column'.
/// 2. If column name can't be short cause of alias with same name generate and use unique name for it.
/// 3. If column clashes with another column generate and use unique names for them.
/// 4. If column clashes with another column and it's short - it's 'ambiguous column' error.
/// 5. If column clashes with alias add short column name to select list. It would be removed later if not needed.
std::vector<TableNeededColumns> normalizeColumnNamesExtractNeeded(
    const std::vector<TableWithColumnNamesAndTypes> & tables,
    const Aliases & aliases,
    const std::vector<ASTIdentifier *> & identifiers,
    const std::unordered_set<ASTIdentifier *> & public_identifiers,
    UniqueShortNames & unique_names)
{
    size_t last_table_pos = tables.size() - 1;

    NameSet restored_names;
    std::vector<TableNeededColumns> needed_columns;
    needed_columns.reserve(tables.size());
    for (auto & table : tables)
        needed_columns.push_back(TableNeededColumns{table.table});

    for (ASTIdentifier * ident : identifiers)
    {
        bool got_alias = aliases.count(ident->name);

        if (auto table_pos = IdentifierSemantic::chooseTable(*ident, tables))
        {
            if (!ident->isShort())
            {
                if (got_alias)
                    throw Exception("Alias clashes with qualified column '" + ident->name + "'", ErrorCodes::AMBIGUOUS_COLUMN_NAME);

                String short_name = ident->shortName();
                String original_long_name;
                if (public_identifiers.count(ident))
                    original_long_name = ident->name;

                size_t count = countTablesWithColumn(tables, short_name);

                if (count > 1 || aliases.count(short_name))
                {
                    auto & table = tables[*table_pos];
                    IdentifierSemantic::setColumnLongName(*ident, table.table); /// table.column -> table_alias.column
                    auto & unique_long_name = ident->name;

                    /// For tables moved into subselects we need unique short names for clashed names
                    if (*table_pos != last_table_pos)
                    {
                        String unique_short_name = unique_names.longToShort(unique_long_name);
                        ident->setShortName(unique_short_name);
                        needed_columns[*table_pos].column_clashes.emplace(short_name, unique_short_name);
                    }
                }
                else
                {
                    ident->setShortName(short_name); /// table.column -> column
                    needed_columns[*table_pos].no_clashes.emplace(short_name);
                }

                restoreName(*ident, original_long_name, restored_names);
            }
            else if (got_alias)
                needed_columns[*table_pos].alias_clashes.emplace(ident->shortName());
            else
                needed_columns[*table_pos].no_clashes.emplace(ident->shortName());
        }
        else if (!got_alias)
            throw Exception("Unknown column name '" + ident->name + "'", ErrorCodes::UNKNOWN_IDENTIFIER);
    }

    return needed_columns;
}

/// Make expression list for current subselect
std::shared_ptr<ASTExpressionList> subqueryExpressionList(
    size_t table_pos,
    const std::vector<TableNeededColumns> & needed_columns,
    const std::vector<std::vector<ASTPtr>> & alias_pushdown)
{
    auto expression_list = std::make_shared<ASTExpressionList>();

    /// First time extract needed left table columns manually.
    /// Next times extract left table columns via QualifiedAsterisk: `--s`.*
    if (table_pos == 1)
        needed_columns[0].fillExpressionList(*expression_list);
    else
        expression_list->children.emplace_back(makeSubqueryQualifiedAsterisk());

    /// Add needed right table columns
    needed_columns[table_pos].fillExpressionList(*expression_list);

    for (auto & expr : alias_pushdown[table_pos])
        expression_list->children.emplace_back(std::move(expr));

    return expression_list;
}

} /// namelesspace


void JoinToSubqueryTransformMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = ast->as<ASTSelectQuery>())
    {
        if (data.version == 1)
            visitV1(*t, ast, data);
        else
            visitV2(*t, ast, data);
    }
}

/// The reason for V2: not to alias columns without clashes.
/// It allows USING and 'select *' for queries with subselects. It doesn't need AsterisksSemantic and related stuff.
/// 1. Expand asterisks in select expression list.
/// 2. Normalize column names and find name clashes
/// 3. Rewrite multiple JOINs with subqueries:
///    SELECT ... FROM (SELECT `--.s`.*, ... FROM (...) AS `--.s` JOIN tableY ON ...) AS `--.s` JOIN tableZ ON ...'
/// 4. Push down expressions of aliases used in ON section into expression list of first reletad subquery
void JoinToSubqueryTransformMatcher::visitV2(ASTSelectQuery & select, ASTPtr & ast, Data & data)
{
    std::vector<const ASTTableExpression *> table_expressions;
    if (!needRewrite<2>(select, table_expressions))
        return;

    auto & src_tables = select.tables()->children;
    size_t tables_count = src_tables.size();

    if (table_expressions.size() != data.tables.size() ||
        tables_count != data.tables.size())
        throw Exception("Inconsistent tables count in JOIN rewriter", ErrorCodes::LOGICAL_ERROR);

    /// Replace * and t.* with columns in select expression list.
    {
        ExtractAsterisksVisitor::Data asterisks_data(data.tables);
        ExtractAsterisksVisitor(asterisks_data).visit(select.select());
        if (asterisks_data.new_select_expression_list)
            select.setExpression(ASTSelectQuery::Expression::SELECT, std::move(asterisks_data.new_select_expression_list));
    }

    /// Collect column identifiers

    std::vector<ASTIdentifier *> identifiers;
    CollectColumnIdentifiersVisitor(identifiers).visit(ast);

    std::vector<ASTIdentifier *> using_identifiers;
    std::vector<std::vector<ASTPtr>> alias_pushdown(tables_count);
    std::unordered_map<String, ASTPtr> on_aliases;

    /// Collect columns from JOIN sections. Detect if we have aliases there (they need pushdown).
    for (size_t table_pos = 0; table_pos < tables_count; ++table_pos)
    {
        auto * table = src_tables[table_pos]->as<ASTTablesInSelectQueryElement>();
        if (table->table_join)
        {
            auto & join = table->table_join->as<ASTTableJoin &>();
            if (join.on_expression)
            {
                std::vector<ASTIdentifier *> on_identifiers;
                CollectColumnIdentifiersVisitor(on_identifiers).visit(join.on_expression);
                identifiers.insert(identifiers.end(), on_identifiers.begin(), on_identifiers.end());

                /// Extract aliases used in ON section for pushdown. Exclude the last table.
                if (table_pos < tables_count - 1)
                {
                    for (auto * ident : on_identifiers)
                    {
                        auto it = data.aliases.find(ident->name);
                        if (!on_aliases.count(ident->name) && it != data.aliases.end())
                        {
                            auto alias_expression = it->second;
                            alias_pushdown[table_pos].push_back(alias_expression);
                            on_aliases[ident->name] = alias_expression;
                        }
                    }
                }
            }
            else if (join.using_expression_list)
                CollectColumnIdentifiersVisitor(using_identifiers).visit(join.on_expression);
        }
    }

    /// Check if alias expression is too complex to push it down.
    for (auto & expr : on_aliases)
    {
        CheckAliasDependencyVisitor::Data check{data.aliases};
        CheckAliasDependencyVisitor(check).visit(expr.second);
        if (check.dependency)
            throw Exception("Cannot rewrite JOINs. Alias '" + expr.first +
                            "' used in ON section depends on another alias '" + check.dependency->name + "'",
                            ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Check same name in aliases, USING and ON sections. Cannot push down alias to ON through USING cause of name masquerading.
    for (auto * ident : using_identifiers)
        if (on_aliases.count(ident->name))
            throw Exception("Cannot rewrite JOINs. Alias '" + ident->name + "' appears both in ON and USING", ErrorCodes::NOT_IMPLEMENTED);
    using_identifiers.clear();

    /// Replace pushdowned expressions with aliases names in original expression lists.
    RewriteWithAliasVisitor(on_aliases).visit(ast);
    on_aliases.clear();

    /// We need to know if identifier is public. If so we have too keep its output name.
    std::unordered_set<ASTIdentifier *> public_identifiers;
    for (auto & top_level_child : select.select()->children)
        if (auto * ident = top_level_child->as<ASTIdentifier>())
            public_identifiers.insert(ident);

    UniqueShortNames unique_names;
    std::vector<TableNeededColumns> needed_columns =
        normalizeColumnNamesExtractNeeded(data.tables, data.aliases, identifiers, public_identifiers, unique_names);

    /// Rewrite JOINs with subselects

    ASTPtr left_table = src_tables[0];

    static ASTPtr subquery_template = makeSubqueryTemplate();

    for (size_t i = 1; i < src_tables.size() - 1; ++i)
    {
        auto expression_list = subqueryExpressionList(i, needed_columns, alias_pushdown);

        ASTPtr subquery = subquery_template->clone();
        SubqueryExpressionsRewriteVisitor::Data expr_rewrite_data{std::move(expression_list)};
        SubqueryExpressionsRewriteVisitor(expr_rewrite_data).visit(subquery);

        left_table = replaceJoin(left_table, src_tables[i], subquery);
    }

    RewriteVisitor::Data visitor_data{left_table, src_tables.back()};
    RewriteVisitor(visitor_data).visit(select.refTables());

    data.done = true;
}

void JoinToSubqueryTransformMatcher::visitV1(ASTSelectQuery & select, ASTPtr &, Data & data)
{
    using RevertedAliases = AsteriskSemantic::RevertedAliases;

    std::vector<const ASTTableExpression *> table_expressions;
    if (!needRewrite(select, table_expressions))
        return;

    if (table_expressions.size() != data.tables.size())
        throw Exception("Inconsistent tables count in JOIN rewriter", ErrorCodes::LOGICAL_ERROR);

    bool has_subquery = false;
    for (const auto & expr : table_expressions)
        if (expr->subquery)
            has_subquery = true;

    if (!has_subquery)
    {
        ExtractAsterisksVisitor::Data asterisks_data(data.tables);
        ExtractAsterisksVisitor(asterisks_data).visit(select.select());
        if (asterisks_data.new_select_expression_list)
            select.setExpression(ASTSelectQuery::Expression::SELECT, std::move(asterisks_data.new_select_expression_list));
    }

    ColumnAliasesVisitor::Data aliases_data(getDatabaseAndTables(select, ""));
    if (select.select())
    {
        /// TODO: there's a bug here. We need to publish only top-level ASTIdentifiers but visitor extracts all.
        aliases_data.public_names = true;
        ColumnAliasesVisitor(aliases_data).visit(select.select());
        aliases_data.public_names = false;
    }
    if (select.where())
        ColumnAliasesVisitor(aliases_data).visit(select.where());
    if (select.prewhere())
        ColumnAliasesVisitor(aliases_data).visit(select.prewhere());
    if (select.orderBy())
        ColumnAliasesVisitor(aliases_data).visit(select.orderBy());
    if (select.groupBy())
        ColumnAliasesVisitor(aliases_data).visit(select.groupBy());
    if (select.having())
        ColumnAliasesVisitor(aliases_data).visit(select.having());

    /// JOIN sections
    for (auto & child : select.tables()->children)
    {
        auto * table = child->as<ASTTablesInSelectQueryElement>();
        if (table->table_join)
        {
            auto & join = table->table_join->as<ASTTableJoin &>();
            if (join.on_expression)
                ColumnAliasesVisitor(aliases_data).visit(join.on_expression);
        }
    }

    aliases_data.replaceIdentifiersWithAliases();

    auto rev_aliases = std::make_shared<RevertedAliases>();
    rev_aliases->swap(aliases_data.rev_aliases);

    auto & src_tables = select.tables()->children;
    ASTPtr left_table = src_tables[0];

    static ASTPtr subquery_template = makeSubqueryTemplate();

    for (size_t i = 1; i < src_tables.size() - 1; ++i)
    {
        left_table = replaceJoin(left_table, src_tables[i], subquery_template->clone());
        if (!left_table)
            throw Exception("Cannot replace tables with subselect", ErrorCodes::LOGICAL_ERROR);

        /// attach data to generated asterisk
        AppendSemanticVisitor::Data semantic_data{rev_aliases, false};
        AppendSemanticVisitor(semantic_data).visit(left_table);
    }

    /// replace tables in select with generated two-table join
    RewriteVisitor::Data visitor_data{left_table, src_tables.back()};
    RewriteVisitor(visitor_data).visit(select.refTables());

    data.done = true;
}

ASTPtr JoinToSubqueryTransformMatcher::replaceJoin(ASTPtr ast_left, ASTPtr ast_right, ASTPtr subquery_template)
{
    const auto * left = ast_left->as<ASTTablesInSelectQueryElement>();
    const auto * right = ast_right->as<ASTTablesInSelectQueryElement>();
    if (!left || !right)
        throw Exception("Two TablesInSelectQueryElements expected", ErrorCodes::LOGICAL_ERROR);

    if (!right->table_join)
        throw Exception("Table join expected", ErrorCodes::LOGICAL_ERROR);

    /// replace '_t' with pair of joined tables
    RewriteVisitor::Data visitor_data{ast_left, ast_right};
    RewriteVisitor(visitor_data).visit(subquery_template);
    return subquery_template;
}

}
