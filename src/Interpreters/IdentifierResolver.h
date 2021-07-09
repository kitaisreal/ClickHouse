#pragma once

#include <vector>
#include <stack>
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
    extern const int UNKNOWN_IDENTIFIER;
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

    virtual StorageID getStorageID() const = 0;

    /// TODO: Normalize path
    virtual bool hasPath(const IdentifierPath & path) const = 0;

    /// TODO: Normalize path
    virtual DataTypePtr resolvePath(const IdentifierPath & path) const = 0;

    /// TODO: Aliases

    /// TODO: Asterisk

};

class IDatabaseUpdated
{
public:

    virtual ~IDatabaseUpdated() {}

    virtual String getName() const = 0;

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

    static IdentifierPtr createForColumn(const IdentifierPath & path, DataTypePtr data_type, TablePtr table, DatabaseUpdatedPtr database)
    {
        assert(path.size() == 3);

        auto identifier = std::make_shared<Identifier>();

        identifier->type = Type::column;
        identifier->data_type = data_type;
        identifier->path = path;
        identifier->table = std::move(table);
        identifier->database = database;

        return identifier;
    }

    static IdentifierPtr createForTable(const IdentifierPath & path, TablePtr table, DatabaseUpdatedPtr database)
    {
        assert(path.size() == 2);

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

    IdentifierPtr resolveIdentifier(const IdentifierPath & identifier_path)
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
            if (table->hasPath(identifier_path))
            {
                return {};
            }
            else
            {
                return {};
            }
        }
        else if (type == Type::alias)
        {
            if (identifier_path[0] == path[0])
            {
                IdentifierPath resolve_path = alias_identifier->path;

                for (size_t i = 1; i < identifier_path.size(); ++i)
                    resolve_path.emplace_back(identifier_path[i]);

                return alias_identifier->resolveIdentifier(resolve_path);
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

        if (identifier->getTable())
            tables_identifiers.emplace_back(identifier);
    }

    void addUnresolvedIdentifier(const ASTIdentifier * identifier)
    {
        unresolved_identifiers.emplace_back(identifier);
    }

    IdentifierPtr resolveIdentifierFromTables(const IdentifierPath & identifier_path)
    {
        TablePtr resolved_table;
        DataTypePtr identifier_type;

        for (const auto & table : tables_identifiers)
        {
            auto type = table->resolvePath(identifier_path);

            if (resolved_table)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Ambigious identifier {}", toString(identifier_path));

            resolved_table = table;
            identifier_type = type;
        }

        Identifier::createForColumn({resolved_table->database->getName()}, identifier_type, table, table->getDatabase());
        return identifier_type;
    }

    void resolveUnresolvedIdentifiers()
    {
        struct ResolveIdentifierData
        {
            enum ResolveStatus
            {
                unresolved,
                in_resolve_process,
                resolved
            };

            ResolveStatus status;
            IdentifierPtr identifier;
        };

        std::unordered_map<std::string_view, IdentifierPtr> alias_name_to_identifier;
        std::unordered_map<std::string_view, ResolveIdentifierData> unresolved_identifier_to_data;

        for (const auto & identifier : identifiers)
        {
            if (identifier->getType() == Identifier::Type::alias)
                alias_name_to_identifier[toString(identifier->getPath())] = identifier;
        }

        for (const auto & unresolved_identifier : unresolved_identifiers)
        {
            auto alias = unresolved_identifier->tryGetAlias();

            if (!alias.empty())
                unresolved_identifier_to_data[alias] = {ResolveIdentifierData::unresolved, nullptr };
        }

        for (const auto & unresolved_identifier : unresolved_identifiers)
        {
            const auto & name_parts = unresolved_identifier->name_parts;
            auto alias_part = name_parts[0];

            auto resolved_alias_it = alias_name_to_identifier.find(alias_part);
            if (resolved_alias_it != alias_name_to_identifier.end())
            {
                unresolved_identifier_to_data[]
            }
        }


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
            out << identifier->dump() << "\n";

        return out.str();
    }
private:

    ScopeType scope_type;

    std::vector<ScopePtr> inner_scopes;

    std::vector<TablePtr> tables_identifiers;

    std::vector<IdentifierPtr> identifiers;

    std::vector<const ASTIdentifier *> unresolved_identifiers;

    Scope * parent_scope;

};

class IdentifierResolver
{
public:
    struct Settings
    {
        String default_database_name;
        bool prefer_column_name_to_alias;
    };

    explicit IdentifierResolver(
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

    const std::vector<ScopePtr> & getScopes() const
    {
        return scopes;
    }

private:


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
        if (const auto * identifier = expression_part->as<ASTIdentifier>())
        {
            auto scope_identifier = scope->resolveIdentifierFromTables(identifier->name_parts);

            if (scope_identifier)
            {
                scope->addIdentifier(scope_identifier);
                addAliasIntoScopeIfNeeded(scope_identifier, *identifier, scope);
            }
            else
            {
                scope->addUnresolvedIdentifier(identifier);
            }
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

    ASTPtr select_with_union_query;

    DatabaseCatalogPtr database_catalog;

    FunctionCatalogPtr function_catalog;

    Settings settings;

    std::vector<ScopePtr> scopes;
};

}
