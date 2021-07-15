#pragma once

#include <vector>
#include <stack>
#include <string>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <Analyzer/IdentifierPath.h>
#include <Analyzer/Identifier.h>
#include <Analyzer/IDatabaseCatalog.h>
#include <Analyzer/IFunctionCatalog.h>
#include <Analyzer/QueryTree.h>

#include <Interpreters/StorageID.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_IDENTIFIER;
}

class QueryAnalyzer
{
public:
    struct Settings
    {
        String default_database_name;
        bool prefer_column_name_to_alias;
    };

    explicit QueryAnalyzer(
        ASTPtr select_with_union_query_,
        DatabaseCatalogPtr database_catalog_,
        FunctionCatalogPtr function_catalog_,
        const Settings & settings_)
    : select_with_union_query(select_with_union_query_)
    , database_catalog(database_catalog_)
    , function_catalog(function_catalog_)
    , settings(settings_)
    {
        initialize();
    }

    const std::vector<QueryTreePtr> & getQueryTrees() const
    {
        return query_trees;
    }

private:

    void initialize()
    {
        auto * query = select_with_union_query->as<ASTSelectWithUnionQuery>();
        auto & select_lists = query->list_of_selects->as<ASTExpressionList &>();

        for (const auto & select : select_lists.children)
        {
            auto & select_query = select->as<ASTSelectQuery &>();

            std::cerr << select->dumpTree() << std::endl;

            auto query_tree = std::make_shared<QueryTree>("");

            if (auto * tables_ptr = select_query.tables()->as<ASTTablesInSelectQuery>())
                addTableExpressionsIntoTree(*tables_ptr, *query_tree);

            for (size_t i = 0; i < static_cast<size_t>(ASTSelectQuery::Expression::SETTINGS); ++i)
            {
                auto expression_type = static_cast<ASTSelectQuery::Expression>(i);

                if (expression_type == ASTSelectQuery::Expression::TABLES)
                    continue;

                auto expressions_untyped = select_query.getExpression(expression_type, false);

                if (!expressions_untyped)
                    continue;

                if (auto * expression_list = expressions_untyped->as<ASTExpressionList>())
                    addExpressionListIntoTree(*expression_list, *query_tree, expression_type);
                else
                    addExpressionElementIntoTree(expressions_untyped, *query_tree, expression_type);
            }

            query_trees.push_back(std::move(query_tree));
        }

        for (const auto & query_tree : query_trees)
            query_tree->resolveIdentifiers();
    }

    void addTableExpressionsIntoTree(const ASTTablesInSelectQuery & tables, QueryTree & tree)
    {
        for (const auto & table_element_untyped : tables.children)
        {
            const auto & table_element = table_element_untyped->as<ASTTablesInSelectQueryElement &>();

            if (table_element.table_expression)
            {
                const auto & table_expression = table_element.table_expression->as<ASTTableExpression &>();

                if (table_expression.database_and_table_name)
                {
                    const auto & ast_table_identifier = table_expression.database_and_table_name->as<ASTTableIdentifier &>();
                    auto table_id = ast_table_identifier.getTableId();
                    std::string database_name = table_id.database_name;

                    if (database_name.empty())
                        database_name = settings.default_database_name;

                    if (!database_catalog->hasDatabase(database_name))
                        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} doesn't exists", database_name);

                    auto database = database_catalog->getDatabase(database_name);
                    if (!database->hasTable(table_id.table_name))
                        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} doesn't exists", table_id.table_name);

                    auto table = database->getTable(table_id.table_name);
                    tree.addTableExpression(database, table, ast_table_identifier.tryGetAlias());
                }
                else
                {
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported operation exception");
                }
            }
            else
            {
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported operation exception");
            }
        }
    }

    static void addExpressionListIntoTree(const ASTExpressionList & expression_List, QueryTree & tree, ASTSelectQuery::Expression expression)
    {
        for (const auto & expression_element : expression_List.children)
            addExpressionElementIntoTree(expression_element, tree, expression);
    }

    static void addExpressionElementIntoTree(const ASTPtr & expression_part, QueryTree & tree, ASTSelectQuery::Expression expression)
    {
        if (const auto * ast_identifier = expression_part->as<ASTIdentifier>())
        {
            tree.addIdentifier(ast_identifier->name_parts, ast_identifier->tryGetAlias(), expression);
        }
        else if (const auto * ast_literal = expression_part->as<ASTLiteral>())
        {
            tree.addConstant(ast_literal->value, ast_literal->tryGetAlias(), expression);
        }
        else if (const auto * expression_list = expression_part->as<ASTExpressionList>())
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported method");
        }
    }

    ASTPtr select_with_union_query;

    DatabaseCatalogPtr database_catalog;

    FunctionCatalogPtr function_catalog;

    Settings settings;

    /// Multiple in case of UNION
    std::vector<QueryTreePtr> query_trees;

};

}
