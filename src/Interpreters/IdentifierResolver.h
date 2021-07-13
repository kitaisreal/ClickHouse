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

    void resolveAsConstant(Field constant_value_)
    {
        assert(resolved == false);

        type = Type::constant;
        constant_value = std::move(constant_value_);
        resolved = true;
    }

    void resolveAsColumn(const IdentifierPath & path_, DataTypePtr data_type_, TablePtr table_, DatabaseUpdatedPtr database_)
    {
        assert(resolved == false);
        assert(path_.size() >= 3);

        type = Type::column;
        path = path_;
        data_type = data_type_;
        table = std::move(table_);
        database = std::move(database_);

        resolved = true;
    }

    void resolveAsIdentifier(const IdentifierPtr & identifier)
    {
        assert(resolved == false);

        *this = *identifier;
    }

    void resolveAsTable(const IdentifierPath & path_, TablePtr table_, DatabaseUpdatedPtr database_)
    {
        assert(resolved == false);
        assert(path_.size() == 2);

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

    void resolveAsLambdaArgument(String & argument_name)
    {
        assert(resolved == false);

        type = Type::lambda_parameter;
        path = {argument_name};
        resolved = true;
    }

    void resolveAsAlias(IdentifierPtr alias_identifer, const String & alias_name)
    {
        assert(resolved == false);

        type = Type::alias;
        path = {alias_name};
        alias_identifier = alias_identifer;
    }

    Type getType() const
    {
        return type;
    }

    bool isResolved() const
    {
        const auto * identifier = removeAlias();
        return identifier->resolved;
    }

    IdentifierPtr getAliasIdentifier()
    {
        return alias_identifier;
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

    IdentifierPtr resolvePath(const IdentifierPath & identifier_path)
    {
        assert(!path.empty());

        if (type == Type::constant)
        {
            return {};
        }
        else if (type == Type::column)
        {
            /// TODO: Expression
            return {};
        }
        else if (type == Type::table)
        {
            auto resolved_path = table->resolvePath(identifier_path);

            if (resolved_path.normalized_path.empty())
                return {};

            auto resolved_identifier = Identifier::createUnresolved();
            resolved_identifier->resolveAsColumn(resolved_path.normalized_path, resolved_path.identifier_type, table, database);

            return resolved_identifier;
        }
        else if (type == Type::alias)
        {
            if (identifier_path[0] == path[0])
            {
                if (alias_identifier->getTypeIgnoreAlias() == Type::constant && identifier_path.size() == 1)
                    return nestedAlias();

                IdentifierPath resolve_path = alias_identifier->path;

                for (size_t i = 1; i < identifier_path.size(); ++i)
                    resolve_path.emplace_back(identifier_path[i]);

                return alias_identifier->resolvePath(resolve_path);
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
            out << path[0];
            out << ' ';
            out << alias_identifier->dump();
        }

        return out.str();
    }

private:

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

    const Identifier * removeAlias() const
    {
        const Identifier * value = this;

        while (value->type == Identifier::Type::alias)
            value = value->alias_identifier.get();

        return value;
    }

    IdentifierPtr nestedAlias() const
    {
        IdentifierPtr current_alias_identifier = alias_identifier;

        if (current_alias_identifier && current_alias_identifier->getType() == Identifier::Type::alias)
            current_alias_identifier = current_alias_identifier->alias_identifier;

        return current_alias_identifier;
    }

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
    explicit Scope(ScopeType scope_type_, const String & alias_name_)
        : scope_type(scope_type_)
        , alias_name(alias_name_)
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

    IdentifierPtr addConstant(Field constant_value, const String & alias, bool visible_from_parent_scope)
    {
        auto identifier = Identifier::createUnresolved();
        identifier->resolveAsConstant(std::move(constant_value));
        identifiers.emplace_back(identifier);
        auto alias_identifier = addAliasIdentifierIfNeeded(identifier, alias);

        if (visible_from_parent_scope)
        {
            if (alias_identifier)
                visible_from_parent_scope_identifiers.emplace_back(alias_identifier);
            else
                visible_from_parent_scope_identifiers.emplace_back(identifier);
        }

        return identifier;
    }

    IdentifierPtr addIdentifierToResolve(const IdentifierPath & path, const String & alias, bool visible_from_parent_scope)
    {
        auto unresolved_identifier = Identifier::createUnresolved(path);
        unresolved_identifiers.emplace_back(unresolved_identifier);
        auto alias_identifier = addAliasIdentifierIfNeeded(unresolved_identifier, alias);

        if (visible_from_parent_scope)
        {
            if (alias_identifier)
                visible_from_parent_scope_identifiers.emplace_back(alias_identifier);
            else
                visible_from_parent_scope_identifiers.emplace_back(unresolved_identifier);
        }

        return unresolved_identifier;
    }

    IdentifierPtr addTableExpression(DatabaseUpdatedPtr database, TablePtr table, const String & alias)
    {
        auto table_identifier = Identifier::createUnresolved();
        table_identifier->resolveAsTable({database->getName(), table->getStorageID().getTableName()}, table, database);
        table_identifier->setScope(this);
        table_identifiers.emplace_back(table_identifier);

        if (!alias.empty())
        {
            auto alias_identifier = Identifier::createUnresolved();
            alias_identifier->resolveAsAlias(table_identifier, alias);
            alias_identifier->setScope(this);
            table_identifiers.emplace_back(alias_identifier);
        }

        return table_identifier;
    }

    IdentifierPtr resolveVisibleIdentifier(const IdentifierPath & path)
    {
        IdentifierPtr result_identifier;

        for (const auto & visible_identifier : visible_from_parent_scope_identifiers)
        {
            auto identifier = visible_identifier->resolvePath(path);
            if (identifier && result_identifier)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Duplicate in visible identifiers");

            result_identifier = identifier;
        }

        return result_identifier;
    }

    IdentifierPtr tryResolveIdentifierFromAliases(const IdentifierPath & path)
    {
        std::cerr << "Scope::tryResolveIdentifierFromAliases " << this << " path " << toString(path) << std::endl;

        auto it = alias_name_to_alias_identifier.find(path[0]);
        if (it == alias_name_to_alias_identifier.end())
        {
            std::cerr << "No such alias identifier " << std::endl;
            return nullptr;
        }

        auto alias_identifier = it->second;

        if (alias_identifier->isResolved())
        {
            auto identifier = alias_identifier->resolvePath(path);
            std::cerr << "Alias identifier " << toString(alias_identifier->getAliasIdentifier()->getPath()) << std::endl;
            std::cerr << "Alias is resolved " << identifier << std::endl;
            return identifier;
        }
        else
        {
            std::cerr << "Resolve alias identifier " << toString(alias_identifier->getAliasIdentifier()->getPath()) << std::endl;
            resolveIdentifier(alias_identifier->getAliasIdentifier());
            return alias_identifier->getAliasIdentifier();
        }
    }

    IdentifierPtr tryResolveIdentifierFromTables(const IdentifierPath & path)
    {
        IdentifierPtr result_identifier;

        for (auto & table_identifier : table_identifiers)
        {
            auto resolved_identifier = table_identifier->resolvePath(path);
            if (resolved_identifier && result_identifier)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Ambigious resolve identifier {}", toString(path));

            result_identifier = resolved_identifier;
        }

        return result_identifier;
    }

    void resolveIdentifier(IdentifierPtr unresolved_identifier)
    {
        auto it = identifier_to_resolve_status.find(unresolved_identifier.get());
        if (it->second == ResolveStatus::in_resolve_process)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Cyclic aliases for identifier {}", toString(unresolved_identifier->getPath()));

        identifier_to_resolve_status[unresolved_identifier.get()] = ResolveStatus::in_resolve_process;

        auto resolved_from_aliases = tryResolveIdentifierFromAliases(unresolved_identifier->getPath());
        if (resolved_from_aliases)
            unresolved_identifier->resolveAsIdentifier(resolved_from_aliases);

        if (!unresolved_identifier->isResolved())
        {
            auto resolved_from_tables = tryResolveIdentifierFromTables(unresolved_identifier->getPath());
            if (resolved_from_tables)
                unresolved_identifier->resolveAsIdentifier(resolved_from_tables);
        }

        if (!unresolved_identifier->isResolved())
        {
            Scope * parent_scope_to_check = parent_scope;

            while (parent_scope_to_check)
            {
                auto resolved_identifier = parent_scope_to_check->tryResolveIdentifierFromAliases(unresolved_identifier->getPath());
                if (resolved_identifier)
                    unresolved_identifier->resolveAsIdentifier(resolved_identifier);
            }
        }

        if (!unresolved_identifier->isResolved())
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Cannot resolve identifier {}", toString(unresolved_identifier->getPath()));
        }
        else
        {
            identifiers.emplace_back(unresolved_identifier);
            identifier_to_resolve_status[unresolved_identifier.get()] = ResolveStatus::resolved;
        }
    }

    void resolveIdentifiers()
    {
        std::cerr << "Scope::resolveIdentifiers " << std::endl;

        for (auto & identifier : identifiers)
            identifier_to_resolve_status[identifier.get()] = ResolveStatus::resolved;

        for (auto & unresolved_identifier : unresolved_identifiers)
            identifier_to_resolve_status[unresolved_identifier.get()] = ResolveStatus::unresolved;

        for (const auto & inner_scope : inner_scopes)
            inner_scope->resolveIdentifiers();

        while (!unresolved_identifiers.empty())
        {
            auto unresolved_identifier = unresolved_identifiers.back();
            unresolved_identifiers.pop_back();

            /// Identifier potentially can be resolved during inner scope resolveIdentifiers call
            /// or it can be resolved recursively during resolveIdentifier call
            auto resolve_status_it = identifier_to_resolve_status.find(unresolved_identifier.get());
            if (resolve_status_it->second == ResolveStatus::resolved)
                continue;

            resolveIdentifier(unresolved_identifier);
        }

    }

    std::string dump() const
    {
        WriteBufferFromOwnString out;

        out << "Identifiers: " << identifiers.size() << '\n';
        for (const auto & identifier : identifiers)
            out << identifier->dump() << '\n';

        out << "Unresolved identifiers " << unresolved_identifiers.size() << '\n';
        for (const auto & identifier : unresolved_identifiers)
            out << identifier->dump() << '\n';

        out << "Aliases: " << alias_name_to_alias_identifier.size() << '\n';
        for (const auto & [_, identifier] : alias_name_to_alias_identifier)
            out << identifier->dump() << '\n';

        return out.str();
    }

private:

    IdentifierPtr addAliasIdentifierIfNeeded(IdentifierPtr identifier, const String & alias)
    {
        if (alias.empty())
            return nullptr;

        auto alias_identifer = Identifier::createUnresolved();
        alias_identifer->resolveAsAlias(identifier, alias);
        alias_identifer->setScope(this);
        alias_name_to_alias_identifier[alias] = alias_identifer;

        return alias_identifer;
    }

    ScopeType scope_type;

    String alias_name;

    std::vector<IdentifierPtr> table_identifiers;

    std::vector<ScopePtr> inner_scopes;

    std::vector<IdentifierPtr> visible_from_parent_scope_identifiers;

    std::vector<IdentifierPtr> identifiers;

    std::vector<IdentifierPtr> unresolved_identifiers;

    std::unordered_map<std::string, IdentifierPtr> alias_name_to_alias_identifier;

    enum class ResolveStatus
    {
        unresolved,
        resolved,
        in_resolve_process
    };

    std::unordered_map<Identifier *, ResolveStatus> identifier_to_resolve_status;

    Scope * parent_scope = nullptr;

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

    IdentifierPtr getIdentifierFor(ASTIdentifier * identifier)
    {
        auto it = ast_identifier_to_identifier.find(identifier);
        if (it == ast_identifier_to_identifier.end())
            return nullptr;

        return it->second;
    }

    IdentifierPtr getIdentifierFor(ASTLiteral * literal)
    {
        auto it = ast_literal_to_identifier.find(literal);
        if (it == ast_literal_to_identifier.end())
            return nullptr;

        return it->second;
    }

private:

    void initialize()
    {
        auto * query = select_with_union_query->as<ASTSelectWithUnionQuery>();
        auto & select_lists = query->list_of_selects->as<ASTExpressionList &>();

        for (const auto & select : select_lists.children)
        {
            auto & select_query = select->as<ASTSelectQuery &>();

            ScopePtr scope = std::make_shared<Scope>(ScopeType::subquery, "");

            if (auto * tables_ptr = select_query.tables()->as<ASTTablesInSelectQuery>())
                addTableExpressionsIntoScope(*tables_ptr, scope);

            for (size_t i = 0; i < static_cast<size_t>(ASTSelectQuery::Expression::SETTINGS); ++i)
            {
                auto expression_type = static_cast<ASTSelectQuery::Expression>(i);

                if (expression_type == ASTSelectQuery::Expression::TABLES)
                    continue;

                bool visible_from_parent_scope = expression_type == ASTSelectQuery::Expression::SELECT;
                auto expressions_untyped = select_query.getExpression(expression_type, false);

                if (!expressions_untyped)
                    continue;

                if (auto * expression_list = expressions_untyped->as<ASTExpressionList>())
                    addExpressionListIntoScope(*expression_list, scope, visible_from_parent_scope);
                else
                    addExpressionElementIntoScope(expressions_untyped, scope, visible_from_parent_scope);
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

    void addExpressionListIntoScope(const ASTExpressionList & expression_List, ScopePtr & scope, bool visible_from_parent_scope)
    {
        for (const auto & expression_element : expression_List.children)
            addExpressionElementIntoScope(expression_element, scope, visible_from_parent_scope);
    }

    void addExpressionElementIntoScope(const ASTPtr & expression_part, ScopePtr & scope, bool visible_from_parent_scope)
    {
        if (const auto * ast_identifier = expression_part->as<ASTIdentifier>())
        {
            auto identifier = scope->addIdentifierToResolve(ast_identifier->name_parts, ast_identifier->tryGetAlias(), visible_from_parent_scope);
            ast_identifier_to_identifier.emplace(ast_identifier, std::move(identifier));
        }
        else if (const auto * ast_literal = expression_part->as<ASTLiteral>())
        {
            auto identifier = scope->addConstant(ast_literal->value, ast_literal->tryGetAlias(), visible_from_parent_scope);
            ast_literal_to_identifier.emplace(ast_literal, std::move(identifier));
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

    std::unordered_map<const ASTIdentifier *, IdentifierPtr> ast_identifier_to_identifier;

    std::unordered_map<const ASTLiteral *, IdentifierPtr> ast_literal_to_identifier;
};

}
