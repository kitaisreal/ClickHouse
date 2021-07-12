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
#include <Parsers/ASTFunction.h>
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

class IExpression;
using ExpressionPtr = std::shared_ptr<IExpression>;

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

struct IdentifierResolvedResult
{
    IdentifierPath normalized_path;
    DataTypePtr identifier_type;
};

class ITable
{
public:

    virtual ~ITable() {}

    virtual StorageID getStorageID() const = 0;

    virtual bool hasPath(const IdentifierPath & path) const = 0;

    virtual IdentifierResolvedResult resolvePath(const IdentifierPath & path) const = 0;

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

    static IdentifierPtr createUnresolved()
    {
        auto identifier = std::make_shared<Identifier>();
        return identifier;
    }

    static IdentifierPtr createUnresolved(const IdentifierPath & path_)
    {
        auto identifier = std::make_shared<Identifier>();

        identifier->path = path_;

        return identifier;
    }

    // void updatePath(const IdentifierPath & path_)
    // {
    //     assert(resolved == false);
    //     path = path_;
    // }

    void resolveAsConstant(Field constant_value_)
    {
        assert(resolved == false);

        type = Type::constant;
        constant_value = std::move(constant_value_);
        resolved = true;
    }

    // static IdentifierPtr createForConstant(Field constant_value)
    // {
    //     auto identifier = std::make_shared<Identifier>();

    //     identifier->type = Type::constant;
    //     identifier->constant_value = std::move(constant_value);

    //     return identifier;
    // }

    void resolveAsColumn(const IdentifierPath & path_, DataTypePtr data_type_, TablePtr table_, DatabaseUpdatedPtr database_)
    {
        assert(resolved == false);
        assert(path.size() >= 3);

        type = Type::column;
        path = path_;
        data_type = data_type_;
        table = std::move(table_);
        database = std::move(database_);

        resolved = true;
    }

    // static IdentifierPtr createForColumn(const IdentifierPath & path, DataTypePtr data_type, TablePtr table, DatabaseUpdatedPtr database)
    // {
    //     assert(path.size() >= 3);

    //     auto identifier = std::make_shared<Identifier>();

    //     identifier->type = Type::column;
    //     identifier->data_type = data_type;
    //     identifier->path = path;
    //     identifier->table = std::move(table);
    //     identifier->database = database;

    //     return identifier;
    // }

    void resolveAsTable(const IdentifierPath & path_, TablePtr table_, DatabaseUpdatedPtr database_)
    {
        assert(resolved == false);
        assert(path.size() == 2);

        type = Type::table;
        path = path_;
        table = table_;
        database = database_;
        resolved = true;
    }

    void resolveAsExpression(ExpressionPtr expression_)
    {
        assert(resolved == false);

        type = Type::expression;
        expression = expression_;
        resolved = true;
    }

    // static IdentifierPtr createForTable(const IdentifierPath & path, TablePtr table, DatabaseUpdatedPtr database)
    // {
    //     assert(path.size() == 2);

    //     auto identifier = std::make_shared<Identifier>();

    //     identifier->type = Type::table;
    //     identifier->path = path;
    //     identifier->table = std::move(table);
    //     identifier->database = std::move(database);

    //     return identifier;
    // }

    // static IdentifierPtr createForExpression(ExpressionPtr expression)
    // {
    //     auto identifier = std::make_shared<Identifier>();

    //     identifier->type = Type::expression;
    //     identifier->expression = std::move(expression);

    //     return identifier;
    // }

    void resolveAsLambdaArgument(String & argument_name)
    {
        assert(resolved == false);

        type = Type::lambda_parameter;
        path = {argument_name};
        resolved = true;
    }

    // static IdentifierPtr createForLambdaArgument(const IdentifierPath & path)
    // {
    //     auto identifier = std::make_shared<Identifier>();

    //     identifier->type = Type::lambda_parameter;
    //     identifier->path = path;

    //     return identifier;
    // }

    void resolveAsAlias(IdentifierPtr alias_identifer, const String & alias_name)
    {
        assert(resolved == false);

        type = Type::alias;
        path = {alias_name};
        alias_identifier = alias_identifer;
        resolved = true;
    }

    // static IdentifierPtr createForAlias(IdentifierPtr alias_identifer, const IdentifierPath & path)
    // {
    //     auto identifier = std::make_shared<Identifier>();

    //     identifier->type = Type::alias;
    //     identifier->path = path;
    //     identifier->alias_identifier = std::move(alias_identifer);

    //     return identifier;
    // }

    Type getType() const
    {
        return type;
    }

    bool isResolved() const
    {
        return resolved;
    }

    Type getTypeIgnoreAlias() const
    {
        const Identifier * value = this;

        while (value->type == Identifier::Type::alias)
            value = value->alias_identifier.get();

        return value->type;
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

    // IdentifierPtr resolvePath(const IdentifierPath & identifier_path)
    // {
    //     assert(!path.empty());

    //     if (type == Type::constant)
    //     {
    //         return {};
    //     }
    //     else if (type == Type::column || type == Type::table)
    //     {
    //         auto resolved_path = table->resolvePath(identifier_path);

    //         if (resolved_path.normalized_path.empty())
    //             return {};

    //         auto resolved_identifier = Identifier::createUnresolved();
    //         resolved_identifier->resolveAsColumn(resolved_path.normalized_path, resolved_path.identifier_type, table, database);
    //         return resolved_identifier;
    //     }
    //     else if (type == Type::alias)
    //     {
    //         if (identifier_path[0] == path[0])
    //         {
    //             if (identifier_path.size() == 1)
    //                 return alias_identifier->alias_identifier;

    //             IdentifierPath resolve_path = alias_identifier->path;

    //             for (size_t i = 1; i < identifier_path.size(); ++i)
    //                 resolve_path.emplace_back(identifier_path[i]);

    //             return alias_identifier->resolvePath(resolve_path);
    //         }
    //         else
    //         {
    //             return {};
    //         }
    //     }
    //     else
    //     {
    //         throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported method");
    //     }
    // }

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
            out << path[0];
            out << ' ';
            out << alias_identifier->dump();
        }

        return out.str();
    }

    /// Type of identifier
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

    /// Scope for identifier
    Scope *scope = nullptr;

    /// Is resolved
    bool resolved = false;

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

class IExpression
{
public:

    using ExpressionArguments = std::vector<IdentifierPtr>;

    virtual ~IExpression() {}

    virtual ExpressionArguments getArguments() const = 0;

    virtual DataTypePtr getResultType() const = 0;

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

    using IdentifierKey = size_t;

    IdentifierKey addConstant(Field constant_value, const String & alias)
    {
        size_t identifier_key = current_identifier_key;
        ++current_identifier_key;

        auto identifier = Identifier::createUnresolved();
        identifier->resolveAsConstant(std::move(constant_value));
        addAliasIdentifierIfNeeded(identifier, alias);

        return identifier_key;
    }

    IdentifierKey addIdentifierToResolve(const IdentifierPath & path, const String & alias)
    {
        size_t identifier_key = current_identifier_key;
        ++current_identifier_key;

        auto unresolved_identifier = Identifier::createUnresolved(path);
        unresolved_identifiers.emplace_back(unresolved_identifier);
        addAliasIdentifierIfNeeded(unresolved_identifier, alias);

        return identifier_key;
    }

    IdentifierKey addTableExpression(DatabaseUpdatedPtr database, TablePtr table, const String & alias)
    {
        size_t identifier_key = current_identifier_key;
        ++current_identifier_key;

        auto table_identifier = Identifier::createUnresolved();
        table_identifier->resolveAsTable({database->getName(), table->getStorageID().getTableName()}, table, database);
        table_identifier->setScope(this);
        table_identifiers.emplace_back(table_identifier);

        if (!alias.empty())
        {
            auto alias_identifer = Identifier::createUnresolved();
            alias_identifer->resolveAsAlias(table_identifier, alias);
            alias_identifer->setScope(this);
            table_identifiers.emplace_back(alias_identifer);
        }

        return identifier_key;
    }

    IdentifierPtr tryResolveIdentifierFromAliases(const IdentifierPath & path)
    {
        auto it = alias_name_to_alias_identifier.find(path[0]);
        if (it == alias_name_to_alias_identifier.end())
            return nullptr;

        auto alias_element = it->second;
        auto alias_identifier = alias_element->alias_identifier;

        if (alias_identifier->isResolved())
        {
            /// TODO: Try resolve path from inner identifier
            return nullptr;
        }
        else
        {
            /// TODO: Resolve this identifier
        }

        return nullptr;
    }

    void resolveIdentifiers()
    {
        for (auto & identifier : identifiers)
            identifier_ptr_to_resolve_status[identifier.get()] = resolved;

        for (auto & unresolved_identifier : unresolved_identifiers)
            identifier_ptr_to_resolve_status[unresolved_identifier.get()] = unresolved;

        while (!unresolved_identifiers.empty())
        {
            auto unresolved_identifier = unresolved_identifiers.back();
            unresolved_identifiers.pop_back();

            auto resolve_status_it = identifier_ptr_to_resolve_status.find(unresolved_identifier.get());
            if (resolve_status_it->second == resolved)
                continue;

            auto it = alias_name_to_alias_identifier.find(unresolved_identifier->getPath()[0]);
            if (it != alias_name_to_alias_identifier.end())
            {
                auto & alias_identifier = it->second;
                auto inner_identifier = alias_identifier->alias_identifier;

                if (inner_identifier->isResolved())
                {
                    /// TODO: Try resolve path from this inner identifier
                    // auto result = alias_identifier->resolvePath(unresolved_identifier->getPath());
                    // if (!result.normalized_path.empty())
                    // {
                    //     unresolved_identifier.resolve
                    // }
                }
                else
                {
                    unresolved_identifiers.emplace_back(alias_identifier);
                }
            }
        }
    }

    std::string dump() const
    {
        WriteBufferFromOwnString out;

        for (const auto & identifier : identifiers)
            out << identifier->dump() << "\n";

        return out.str();
    }

private:

    void addAliasIdentifierIfNeeded(IdentifierPtr identifier, const String & alias)
    {
        if (alias.empty())
            return;

        auto alias_identifer = Identifier::createUnresolved();
        alias_identifer->resolveAsAlias(identifier, alias);
        alias_identifer->setScope(this);
        alias_name_to_alias_identifier[alias] = alias_identifer;
    }

    ScopeType scope_type;

    std::vector<IdentifierPtr> table_identifiers;

    std::vector<ScopePtr> inner_scopes;

    std::vector<IdentifierPtr> identifiers;

    std::vector<IdentifierPtr> unresolved_identifiers;

    std::unordered_map<std::string, IdentifierPtr> alias_name_to_alias_identifier;

    enum ResolveStatus
    {
        unresolved,
        resolved,
        in_resolve_process
    };

    std::unordered_map<Identifier *, ResolveStatus> identifier_ptr_to_resolve_status;

    std::vector<IdentifierPtr>

    Scope * parent_scope;

    IdentifierKey current_identifier_key = 0;

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
        auto * query = select_with_union_query->as<ASTSelectWithUnionQuery>();
        auto & select_lists = query->list_of_selects->as<ASTExpressionList &>();

        for (const auto & select : select_lists.children)
        {
            auto & select_query = select->as<ASTSelectQuery &>();

            ScopePtr scope = std::make_shared<Scope>(ScopeType::subquery);

            if (auto * tables_ptr = select_query.tables()->as<ASTTablesInSelectQuery>())
                addTableExpressionsIntoScope(*tables_ptr, scope);

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

        for (const auto & scope : scopes)
            scope->resolveIdentifiers();
    }

    void addTableExpressionsIntoScope(const ASTTablesInSelectQuery & tables, ScopePtr & scope)
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
                    scope->addTableExpression(database, table, table_expression.tryGetAlias());
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

    void addExpressionListIntoScope(const ASTExpressionList & expression_List, ScopePtr & scope)
    {
        for (const auto & expression_element : expression_List.children)
            addExpressionElementIntoScope(expression_element, scope);
    }

    void addExpressionElementIntoScope(const ASTPtr & expression_part, ScopePtr & scope)
    {
        if (const auto * identifier = expression_part->as<ASTIdentifier>())
        {
            scope->addIdentifierToResolve(identifier->name_parts, identifier->tryGetAlias());
           // auto scope_identifier = scope->resolveIdentifierFromTables(identifier->name_parts);

            // if (scope_identifier)
            // {
            //     scope->addIdentifier(scope_identifier);
            //     addAliasIntoScopeIfNeeded(scope_identifier, *identifier, scope);
            // }
            // else
            // {
            //     scope->addUnresolvedIdentifier(identifier);
            // }
        }
        else if (const auto * select_literal = expression_part->as<ASTLiteral>())
        {
            scope->addConstant(select_literal->value, select_literal->tryGetAlias());
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

    std::vector<ScopePtr> scopes;

    struct IdentifierScopeAndKey
    {
        Scope * scope;
        Scope::IdentifierKey key;
    };

    std::unordered_map<IAST *, IdentifierScopeAndKey> ast_node_to_identifier_data;
};

}
