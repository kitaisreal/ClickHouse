#pragma once

#include <vector>
#include <string>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/StorageID.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
}

class Scope;

class ITable;
using TablePtr = std::shared_ptr<const ITable>;

class IDatabaseUpdated;
using DatabaseUpdatedPtr = std::shared_ptr<IDatabaseUpdated>;

class Expression;
using ExpressionPtr = std::shared_ptr<Expression>;

using IdentifierPath = std::vector<std::string>;

IdentifierPath identifierPathFromString(const std::string & value)
{
    IdentifierPath path;
    size_t previous_dot_index = 0;

    for (size_t i = 0; i < value.size(); ++i)
    {
        if (value[i] == '.')
        {
            path.emplace_back(value, previous_dot_index, i);
            previous_dot_index = i + 1;
        }
    }

    path.emplace_back(value, previous_dot_index, value.size());

    return path;
}

std::string toString(const IdentifierPath & path)
{
    WriteBufferFromOwnString buffer;

    for (const auto & path_part : path)
    {
        buffer << path_part;
        buffer << '.';
    }

    if (!path.empty())
        --buffer.position();

    return buffer.str();
}

class Identifier;
using IdentifierPtr = std::shared_ptr<Identifier>;

class ITable
{
public:

    virtual ~ITable() {}

    virtual std::vector<IdentifierPtr> resolveIdentifiers(const IdentifierPath & path) const = 0;

    /// TODO: Aliases

};

class IDatabaseUpdated
{
public:

    virtual ~IDatabaseUpdated() {}

    virtual bool hasTable(const String & table) const = 0;

    virtual TablePtr getTable(const String & table) const = 0;

};

class Identifier
{
public:

    enum class Type
    {
        constant,
        column,
        table,
        expression,
        lambda_parameter,
        alias
    };

    static const char * identifierTypeToString(Type identifier_type)
    {
        switch (identifier_type)
        {
            case Type::constant:
                return "Constant";
            case Type::column:
                return "Column";
            case Type::table:
                return "Table";
            case Type::expression:
                return "Expression";
            case Type::lambda_parameter:
                return "Labmda parameter";
            case Type::alias:
                return "Alias";
        }

        __builtin_unreachable();
    }


    static IdentifierPtr createForConstant(Field constant_value)
    {
        auto identifier = std::make_shared<Identifier>();

        identifier->type = Type::constant;
        identifier->constant_value = std::move(constant_value);

        return identifier;
    }

    static IdentifierPtr createForColumn(const IdentifierPath & path, DataTypePtr data_type, TablePtr table)
    {
        auto identifier = std::make_shared<Identifier>();

        identifier->type = Type::column;
        identifier->data_type = data_type;
        identifier->path = path;
        identifier->table = std::move(table);

        return identifier;
    }

    static IdentifierPtr createForTable(const IdentifierPath & path, TablePtr table, DatabaseUpdatedPtr database)
    {
        auto identifier = std::make_shared<Identifier>();

        identifier->type = Type::table;
        identifier->path = path;
        identifier->table = std::move(table);
        identifier->database = std::move(database);

        return identifier;
    }

    static IdentifierPtr createForExpression(ExpressionPtr expression)
    {
        auto identifier = std::make_shared<Identifier>();

        identifier->type = Type::expression;
        identifier->expression = std::move(expression);

        return identifier;
    }

    static IdentifierPtr createForLambdaArgument(const IdentifierPath & path)
    {
        auto identifier = std::make_shared<Identifier>();

        identifier->type = Type::lambda_parameter;
        identifier->path = path;

        return identifier;
    }

    static IdentifierPtr createForAlias(IdentifierPtr alias_identifer, const IdentifierPath & path)
    {
        auto identifier = std::make_shared<Identifier>();

        identifier->type = Type::alias;
        identifier->path = path;
        identifier->alias_identifier = std::move(alias_identifer);

        return identifier;
    }

    Type getType() const
    {
        return type;
    }

    const IdentifierPath & getPath() const
    {
        return path;
    }

    const DataTypePtr & getDataType() const
    {
        if (type == Type::alias)
            return alias_identifier->getDataType();

        return data_type;
    }

    const Scope * getScope() const
    {
        return scope;
    }

    void setScope(Scope * scope_)
    {
        scope = scope_;
    }

    TablePtr getTable() const
    {
        if (type == Type::alias)
            return alias_identifier->getTable();

        return table;
    }

    DatabaseUpdatedPtr getDatabase() const
    {
        if (type == Type::alias)
            return alias_identifier->getDatabase();

        return database;
    }

    ExpressionPtr getExpression() const
    {
        return expression;
    }

    std::vector<IdentifierPtr> resolveIdentifiers(const IdentifierPath & identifier_path) const
    {
        assert(!path.empty());

        if (type == Type::constant)
        {
            return {};
        }
        else if (type == Type::column)
        {
            /// TODO: Nested identifiers are not yet supported
            return {};
        }
        else if (type == Type::table)
        {
            return getTable()->resolveIdentifiers(identifier_path);
        }
        else if (type == Type::alias)
        {
            if (identifier_path[0] == path[0])
            {
                IdentifierPath resolve_path = alias_identifier->path;

                for (size_t i = 1; i < identifier_path.size(); ++i)
                    resolve_path.emplace_back(identifier_path[i]);

                return alias_identifier->resolveIdentifiers(resolve_path);
            }
            else
            {
                return {};
            }
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported method");
        }
    }

    std::string dump() const
    {
        WriteBufferFromOwnString out;

        out << identifierTypeToString(type);
        out << ' ';

        if (type == Type::constant)
        {
            out << constant_value.dump();
        }
        else if (type == Type::column || type == Type::table || type == Type::lambda_parameter)
        {
            out << concatenatePath(path);
            out << ' ';

            if (data_type)
                out << data_type->getName();
        }
        else if (type == Type::expression)
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported method");
        }
        else if (type == Type::alias)
        {
            out << concatenatePath(path);
            out << ' ';
            out << alias_identifier->dump();
        }

        return out.str();
    }

private:
    Type type;

    /// Valid for constant
    Field constant_value;

    /// Valid for column, constant, expression identifier
    DataTypePtr data_type;

    /// Valid for column, table, lambda parameter identifier
    IdentifierPath path;

    /// Valid for column, table identifier
    TablePtr table;
    DatabaseUpdatedPtr database;

    /// Valid for expression identifier
    ExpressionPtr expression;

    /// Valid for alias identifier
    IdentifierPtr alias_identifier;

    Scope *scope = nullptr;

    static std::string concatenatePath(const IdentifierPath & path)
    {
        std::string result;

        for (const auto & path_part : path)
        {
            result += path_part;
            result += '.';
        }

        if (!path.empty())
            result.pop_back();

        return result;
    }
};

class IDatabaseCatalog
{
public:

    virtual ~IDatabaseCatalog() {}

    virtual bool hasDatabase(const std::string & database_name) const = 0;

    virtual DatabaseUpdatedPtr getDatabase(const std::string & database_name) const = 0;

};

using DatabaseCatalogPtr = std::shared_ptr<IDatabaseCatalog>;

class IFunctionCatalog
{
public:

    virtual ~IFunctionCatalog() {}

    bool has(const String & function_name) const;

    bool infer(const String & function_name, std::vector<DataTypePtr> argument_types) const;

};

using FunctionCatalogPtr = std::shared_ptr<IFunctionCatalog>;

class Expression
{
public:

    using ExpressionArguments = std::vector<IdentifierPtr>;

    explicit Expression(ExpressionArguments arguments_, DataTypePtr result_type_)
        : arguments(std::move(arguments_))
        , result_type(std::move(result_type_))
    {}

    ExpressionArguments getArguments() const
    {
        return arguments;
    }

    DataTypePtr getResultType() const
    {
        return result_type;
    }

private:
    std::vector<IdentifierPtr> arguments;
    DataTypePtr result_type;
};

enum ScopeType
{
    subquery,
    expression
};

class Scope;
using ScopePtr = std::shared_ptr<Scope>;

class Scope
{
public:
    explicit Scope(ScopeType scope_type_)
        : scope_type(scope_type_)
    {}

    ScopeType getScopeType() const
    {
        return scope_type;
    }

    void addScope(ScopePtr scope)
    {
        assert(scope->parent_scope != nullptr);
        scope->parent_scope = this;
        inner_scopes.emplace_back(std::move(scope));
    }

    void addIdentifier(IdentifierPtr identifier)
    {
        identifier->setScope(this);
        identifiers.emplace_back(identifier);

        auto table = identifier->getTable();
        if (table)
            tables.emplace_back(std::move(table));
    }

    std::vector<IdentifierPtr> resolveIdentifiers(const IdentifierPath & identifier_path)
    {
        std::vector<IdentifierPtr> result_identifiers;
        std::set<TablePtr> tables_to_skip;

        for (const auto & identifier : identifiers)
        {
            if (identifier->getPath() == identifier_path)
            {
                /// Do not check associated table if identifier from that table match identifier path

                auto table = identifier->getTable();
                if (table)
                    tables_to_skip.insert(table);

                result_identifiers.push_back(identifier);
                continue;
            }

            if (identifier->getType() == Identifier::Type::table)
                continue;

            auto resolved_identifiers = identifier->resolveIdentifiers(identifier_path);

            for (const auto & resolved_identifier : resolved_identifiers)
                result_identifiers.emplace_back(resolved_identifier);
        }

        for (const auto & identifier : identifiers)
        {
            if (identifier->getType() != Identifier::Type::table)
                continue;

            if (tables_to_skip.contains(identifier->getTable()))
                continue;

            auto resolved_identifiers = identifier->resolveIdentifiers(identifier_path);

            for (const auto & resolved_identifier : resolved_identifiers)
                result_identifiers.emplace_back(resolved_identifier);
        }

        return result_identifiers;
    }

    IdentifierPtr tryResolveIdentifier(const IdentifierPath & identifier_path)
    {
        auto resolved_identifiers = resolveIdentifiers(identifier_path);
        return resolved_identifiers.empty() ? nullptr : resolved_identifiers[0];
    }

    IdentifierPtr resolveIdentifier(const IdentifierPath & identifier_path)
    {
        auto resolved_identifiers = resolveIdentifiers(identifier_path);

        if (resolved_identifiers.empty())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unknown identifier {}", toString(identifier_path));

        if (resolved_identifiers.size() > 1)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Ambigous identifier {}", toString(identifier_path));

        return resolved_identifiers[0];
    }

    IdentifierPtr tryResolveIdentifier(const String & identifier_path)
    {
        auto path = identifierPathFromString(identifier_path);
        return tryResolveIdentifier(path);
    }

    IdentifierPtr resolveIdentifier(const String & identifier_path)
    {
        auto path = identifierPathFromString(identifier_path);
        return resolveIdentifier(path);
    }

    Scope * getParentScope() const
    {
        return parent_scope;
    }

    bool isVisibleFrom(ScopePtr child_scope) const
    {
        Scope * scope_parent = child_scope->parent_scope;

        while (scope_parent != nullptr)
        {
            if (scope_parent == this)
                return true;

            scope_parent = scope_parent->parent_scope;
        }

        return false;
    }

    std::string dump() const
    {
        WriteBufferFromOwnString out;

        for (const auto & identifier : identifiers)
        {
            out << identifier->dump() << "\n";
        }

        return out.str();
    }
private:

    ScopeType scope_type;

    std::vector<ScopePtr> inner_scopes;

    std::vector<TablePtr> tables;

    std::vector<IdentifierPtr> identifiers;

    Scope * parent_scope;

};

class IdentifierResolver
{
public:
    explicit IdentifierResolver(
        ASTPtr select_with_union_query_,
        DatabaseCatalogPtr database_catalog_,
        FunctionCatalogPtr function_catalog_)
    : select_with_union_query(select_with_union_query_)
    , database_catalog(database_catalog_)
    , function_catalog(function_catalog_)
    {
        initialize();
    }

    const std::vector<ScopePtr> & getScopes() const
    {
        return scopes;
    }

private:

    ASTPtr select_with_union_query;

    DatabaseCatalogPtr database_catalog;

    FunctionCatalogPtr function_catalog;

    void initialize()
    {
        std::cerr << "IdentifierResolver::initialize" << std::endl;

        auto * query = select_with_union_query->as<ASTSelectWithUnionQuery>();
        auto & select_lists = query->list_of_selects->as<ASTExpressionList &>();

        for (const auto & select : select_lists.children)
        {
            auto & select_query = select->as<ASTSelectQuery &>();

            std::cerr << "Select query " << select_query.dumpTree() << std::endl;

            ScopePtr scope = std::make_shared<Scope>(ScopeType::subquery);

            if (auto * tables_ptr = select_query.tables()->as<ASTTablesInSelectQuery>())
            {
                const auto & tables = *tables_ptr;

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
                                database_name = "default";

                            if (!database_catalog->hasDatabase(database_name))
                                throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} doesn't exists", database_name);

                            auto database = database_catalog->getDatabase(database_name);
                            if (!database->hasTable(table_id.table_name))
                                throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} doesn't exists", table_id.table_name);

                            auto table = database->getTable(table_id.table_name);

                            auto table_identifier = Identifier::createForTable({database_name, table_id.table_name}, std::move(table), std::move(database));
                            scope->addIdentifier(table_identifier);
                            addAliasIntoScopeIfNeeded(table_identifier, ast_table_identifier, scope);
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

            for (size_t i = 0; i < static_cast<size_t>(ASTSelectQuery::Expression::SETTINGS); ++i)
            {
                auto expression_type = static_cast<ASTSelectQuery::Expression>(i);

                if (expression_type == ASTSelectQuery::Expression::TABLES)
                    continue;

                auto expressions_untyped = select_query.getExpression(expression_type, false);

                if (!expressions_untyped)
                    continue;

                if (auto * expression_list = expressions_untyped->as<ASTExpressionList>())
                    addExpressionListIntoScope(*expression_list, scope);
                else
                    addExpressionElementIntoScope(expressions_untyped, scope);
            }

            scopes.push_back(scope);
        }
    }

    static void addExpressionListIntoScope(const ASTExpressionList & expression_List, ScopePtr & scope)
    {
        for (const auto & expression_element : expression_List.children)
            addExpressionElementIntoScope(expression_element, scope);
    }

    static void addExpressionElementIntoScope(const ASTPtr & expression_part, ScopePtr & scope)
    {
        std::cerr << "IdentifierResolver::addExpressionElementIntoScope " << expression_part->dumpTree() << std::endl;

        if (const auto * identifier = expression_part->as<ASTIdentifier>())
        {
            auto scope_identifier = scope->resolveIdentifier(identifier->name_parts);
            scope->addIdentifier(scope_identifier);
            addAliasIntoScopeIfNeeded(scope_identifier, *identifier, scope);
        }
        else if (const auto * select_literal = expression_part->as<ASTLiteral>())
        {
            auto scope_identifier = Identifier::createForConstant(select_literal->value);
            scope->addIdentifier(scope_identifier);
            addAliasIntoScopeIfNeeded(scope_identifier, *select_literal, scope);
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported method");
        }
    }

    static void addAliasIntoScopeIfNeeded(const IdentifierPtr & identifier, const ASTWithAlias & identifier_node, ScopePtr & scope)
    {
        auto alias = identifier_node.tryGetAlias();

        if (!alias.empty())
        {
            auto alias_identifier = Identifier::createForAlias(identifier, {alias});
            scope->addIdentifier(alias_identifier);
        }
    }

    std::vector<ScopePtr> scopes;
};

}
