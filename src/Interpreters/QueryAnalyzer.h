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

class IScope;

class ITable;
using TablePtr = std::shared_ptr<const ITable>;

class IDatabaseUpdated;
using DatabaseUpdatedPtr = std::shared_ptr<IDatabaseUpdated>;

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


enum class ExpressionType
{
    constant,
    identifier
};

class IExpression;
using ExpressionPtr = std::shared_ptr<const IExpression>;

class IExpression
{
public:

    virtual ~IExpression() {}

    virtual ExpressionType getExpressionType() const = 0;

    virtual DataTypePtr getType() const = 0;

    virtual String dump() const = 0;

};

class ConstantExpression;
using ConstantExpressionPtr = std::shared_ptr<const ConstantExpression>;

class ConstantExpression : public IExpression
{
public:

    static ConstantExpressionPtr create(Field value)
    {
        auto expression = std::make_shared<ConstantExpression>();
        expression->value = std::move(value);
        return expression;
    }

    ExpressionType getExpressionType() const override
    {
        return ExpressionType::constant;
    }

    DataTypePtr getType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    String dump() const override
    {
        return value.dump();
    }

    const Field & getConstant() const override
    {
        return value;
    }

private:
    Field value;
};

class Identifier : public IExpression
{
public:

    enum class Type
    {
        column,
        table,
        lambda_parameter,
        alias
    };

    static const char * identifierTypeToString(Type identifier_type)
    {
        switch (identifier_type)
        {
            case Type::column:
                return "Column";
            case Type::table:
                return "Table";
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

    static IdentifierPtr createAlias(ExpressionPtr expression, const String & alias_name)
    {
        auto identifier = createUnresolved();

        identifier->identifier_type = Type::alias;
        identifier->path = {alias_name};
        identifier->expression = expression;
        identifier->resolved = false;

        return identifier;
    }

    static IdentifierPtr createAlias(IdentifierPtr alias_identifer, const String & alias_name)
    {
        auto identifier = createUnresolved();

        identifier->identifier_type = Type::alias;
        identifier->path = {alias_name};
        identifier->alias_identifier = alias_identifer;
        identifier->resolved = false;

        return identifier;
    }

    static IdentifierPtr createForTable(const IdentifierPath & path_, TablePtr table_, DatabaseUpdatedPtr database_)
    {
        assert(path_.size() == 2);

        auto identifier = createUnresolved();

        identifier->identifier_type = Type::table;
        identifier->path = path_;
        identifier->table = table_;
        identifier->database = database_;
        identifier->resolved = true;

        return identifier;
    }

    void resolveAsColumn(const IdentifierPath & path_, DataTypePtr data_type_, TablePtr table_, DatabaseUpdatedPtr database_)
    {
        assert(resolved == false);
        assert(path_.size() >= 3);

        identifier_type = Type::column;
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

    void resolveAsLambdaArgument(String & argument_name)
    {
        assert(resolved == false);

        identifier_type = Type::lambda_parameter;
        path = {argument_name};
        resolved = true;
    }

    ExpressionType getExpressionType() const override
    {
        return ExpressionType::identifier;
    }

    DataTypePtr getType() const override
    {
        return data_type;
    }

    const Identifier & getIdentifier() const override
    {
        return *this;
    }

    Type getIdentifierType() const
    {
        return identifier_type;
    }

    bool isResolved() const
    {
        const auto * identifier = removeAliasIfNeeded();
        return identifier->resolved;
    }

    IdentifierPtr getAliasIdentifier() const
    {
        return alias_identifier;
    }

    IdentifierPtr removeAlias() const
    {
        IdentifierPtr current_alias_identifier = alias_identifier;

        if (current_alias_identifier && current_alias_identifier->getType() == Identifier::Type::alias)
            current_alias_identifier = current_alias_identifier->alias_identifier;

        return current_alias_identifier;
    }

    Type getTypeRemoveAlias() const
    {
        return removeAlias()->identifier_type;
    }

    const IdentifierPath & getPath() const
    {
        return path;
    }

    const DataTypePtr & getDataType() const
    {
        return removeAliasIfNeeded()->data_type;
    }

    const IScope * getScope() const
    {
        return scope;
    }

    void setScope(IScope * scope_)
    {
        scope = scope_;
    }

    TablePtr getTable() const
    {
        return removeAliasIfNeeded()->table;
    }

    DatabaseUpdatedPtr getDatabase() const
    {
        return removeAliasIfNeeded()->database;
    }

    ExpressionPtr getExpression() const
    {
        return removeAliasIfNeeded()->expression;
    }

    // IdentifierPtr resolvePath(const IdentifierPath & identifier_path)
    // {
    //     assert(!path.empty());

    //     if (type == Type::constant)
    //     {
    //         return {};
    //     }
    //     else if (type == Type::column)
    //     {
    //         /// TODO: Expression
    //         return {};
    //     }
    //     else if (type == Type::table)
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
    //             if (alias_identifier->getTypeIgnoreAlias() == Type::constant && identifier_path.size() == 1)
    //                 return nestedAlias();

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

    std::string dump() const override
    {
        WriteBufferFromOwnString out;

        out << identifierTypeToString(identifier_type);
        out << ' ';

        if (identifier_type == Type::column || identifier_type == Type::table || identifier_type == Type::lambda_parameter)
        {
            out << concatenatePath(path);
            out << ' ';

            if (data_type)
                out << data_type->getName();
        }
        else if (identifier_type == Type::alias)
        {
            out << path[0];
            out << ' ';

            if (alias_identifier)
                out << alias_identifier->dump();
            else
                out << expression->dump();
        }

        return out.str();
    }

private:

    /// Type of identifier
    Type identifier_type;

    /// Valid for column, constant, expression identifier
    DataTypePtr data_type;

    /// Valid for column, table, lambda parameter identifier
    IdentifierPath path;

    /// Valid for column, table identifier
    TablePtr table;
    DatabaseUpdatedPtr database;

    /// Valid for alias identifier. Only expression or alias_identifier can be non null simualteneosly.
    ExpressionPtr expression;
    IdentifierPtr alias_identifier;

    /// Scope for identifier
    IScope *scope = nullptr;

    /// Is resolved
    bool resolved = false;

    const StoragePtr * removeAliasIfNeeded() const
    {
        const Identifier * value = this;

        while (value->identifier_type == Identifier::Type::alias)
            value = value->alias_identifier.get();

        return value;
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

enum ScopeType
{
    query,
    expression
};

class QueryTree;
using QueryTreePtr = std::shared_ptr<QueryTree>;

class IScope
{
public:

    virtual ~IScope() {}

    virtual ScopeType getScopeType() const = 0;

    virtual void resolveIdentifiers() = 0;

    virtual ExpressionPtr tryResolveIdentifierFromAliases(const IdentifierPath & path) = 0;

    IScope * parent_scope;
};

using ScopePtr = std::shared_ptr<IScope>;

class QueryTree : public IScope
{
public:
    explicit QueryTree(const String & alias_name_)
        : alias_name(alias_name_)
    {}

    ScopeType getScopeType() const override
    {
        return ScopeType::query;
    }

    void addInnerScope(ScopePtr scope)
    {
        assert(scope->parent_scope != nullptr);
        scope->parent_scope = this;
        inner_scopes.emplace_back(std::move(scope));
    }

    void addConstant(Field constant_value, const String & alias, ASTSelectQuery::Expression query_expression_part)
    {
        auto constant_expression = ConstantExpression::create(std::move(constant_value));
        auto & expressions = query_expression_type_to_expressions[query_expression_part];

        if (!alias.empty())
        {
            auto alias_identifer = Identifier::createAlias(constant_expression, alias);
            alias_identifer->setScope(this);
            alias_name_to_alias_identifier[alias] = alias_identifer;
            expressions.emplace_back(std::move(alias_identifer));
        }
        else
        {
            expressions.emplace_back(std::move(constant_expression));
        }
    }

    void addIdentifier(const IdentifierPath & path, const String & alias, ASTSelectQuery::Expression query_expression_part)
    {
        auto unresolved_identifier = Identifier::createUnresolved(path);
        unresolved_identifiers.emplace_back(unresolved_identifier);

        auto & expressions = query_expression_type_to_expressions[query_expression_part];

        if (!alias.empty())
        {
            auto alias_identifer = Identifier::createAlias(unresolved_identifier, alias);
            alias_identifer->setScope(this);
            alias_name_to_alias_identifier[alias] = alias_identifer;
            expressions.emplace_back(std::move(alias_identifer));
        }
        else
        {
            expressions.emplace_back(std::move(unresolved_identifier));
        }
    }

    void addTableExpression(DatabaseUpdatedPtr database, TablePtr table, const String & alias)
    {
        std::cerr << "Scope::addTableExpression " << table->getStorageID().getFullNameNotQuoted();
        std::cerr << " alias " << alias << std::endl;

        auto table_identifier = Identifier::createForTable({database->getName(), table->getStorageID().getTableName()}, table, database);
        table_identifier->setScope(this);
        table_identifiers.emplace_back(table_identifier);

        if (!alias.empty())
        {
            auto alias_identifier = Identifier::createAlias(table_identifier, alias);
            alias_identifier->setScope(this);
            table_identifiers.emplace_back(alias_identifier);
        }
    }

    ExpressionPtr tryResolveIdentifierFromAliases(const IdentifierPath & path) override
    {
        std::cerr << "Scope::tryResolveIdentifierFromAliases " << this << " path " << toString(path) << std::endl;

        auto it = alias_name_to_alias_identifier.find(path[0]);
        if (it == alias_name_to_alias_identifier.end())
        {
            std::cerr << "Scope::tryResolveIdentifierFromAliases finish no result" << std::endl;
            return nullptr;
        }

        auto alias_identifier = it->second;

        ExpressionPtr result;

        if (alias_identifier->isResolved())
        {
            std::cerr << "Scope::tryResolveIdentifierFromAliases alias resolved " << toString(alias_identifier->getPath()) << std::endl;
            result = resolvePathWithIdentifier(path, alias_identifier);
        }
        else
        {
            std::cerr << "Scope::tryResolveIdentifierFromAliases alias not resolved start " << toString(alias_identifier->getPath()) << std::endl;
            resolveIdentifier(alias_identifier->getAliasIdentifier());
            std::cerr << "Scope::tryResolveIdentifierFromAliases alias not resolved finished " << toString(alias_identifier->getAliasIdentifier()->getPath()) << std::endl;
            result = resolvePathWithIdentifier(path, alias_identifier);
        }

        std::cerr << "Scope::tryResolveIdentifierFromAliases finished " << result << std::endl;

        return result;
    }

    IdentifierPtr tryResolveIdentifierFromTables(const IdentifierPath & path)
    {
        std::cerr << "Scope::tryResolveIdentifierFromTables " << this << " path " << toString(path) << std::endl;

        IdentifierPtr result_identifier;

        for (auto & table_identifier : table_identifiers)
        {
            auto resolved_identifier = resolvePathWithIdentifier(path, table_identifier);
            if (resolved_identifier && result_identifier)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Ambigious resolve identifier {}", toString(path));

            result_identifier = resolved_identifier;
        }

        std::cerr << "Scope::tryResolveIdentifierFromTables finished " << result_identifier << std::endl;
        return result_identifier;
    }

    ExpressionPtr tryResolveIdentifierFromParentScope(const IdentifierPath & path)
    {
        IScope * parent_scope_to_check = parent_scope;

        while (parent_scope_to_check)
        {
            auto resolved_identifier = parent_scope_to_check->tryResolveIdentifierFromAliases(path);
            if (resolved_identifier)
                return resolved_identifier;
        }

        return nullptr;
    }

    void resolveIdentifier(IdentifierPtr unresolved_identifier)
    {
        std::cerr << "Scope::resolveIdentifier start " << toString(unresolved_identifier->getPath()) << std::endl;

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
            auto resolved_from_parent_scope = tryResolveIdentifierFromParentScope(unresolved_identifier->getPath());

            if (resolved_from_parent_scope)
                unresolved_identifier->resolveAsIdentifier(resolved_from_parent_scope);
        }

        if (!unresolved_identifier->isResolved())
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Cannot resolve identifier {}", toString(unresolved_identifier->getPath()));
        }
        else
        {
            std::cerr << "Scope::resolveIdentifier finished " << toString(unresolved_identifier->getPath()) << std::endl;
            identifiers.emplace_back(unresolved_identifier);
            identifier_to_resolve_status[unresolved_identifier.get()] = ResolveStatus::resolved;
        }
    }

    void resolveIdentifiers() override
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

    IdentifierPtr addAliasForIdentifierIfNeeded(IdentifierPtr identifier, const String & alias)
    {
        if (alias.empty())
            return nullptr;

        auto alias_identifer = Identifier::createAlias(identifier, alias);
        alias_identifer->setScope(this);
        alias_name_to_alias_identifier[alias] = alias_identifer;

        return alias_identifer;
    }

    static ExpressionPtr resolvePathWithIdentifier(const IdentifierPath & path_to_resolve, const IdentifierPtr & identifier)
    {
        assert(!path_to_resolve.empty());

        auto identifier_type = identifier->getIdentifierType();
        const auto & path = identifier->getPath();

        if (identifier_type == Identifier::Type::column)
        {
            auto table = identifier->getTable();

            if (table)
            {
                auto database = identifier->getDatabase();
                auto resolved_path = table->resolvePath(path_to_resolve);

                if (resolved_path.normalized_path.empty())
                    return {};

                auto resolved_identifier = Identifier::createUnresolved();
                resolved_identifier->resolveAsColumn(resolved_path.normalized_path, resolved_path.identifier_type, table, database);
                return resolved_identifier;
            }
            else
            {
                /// TODO: Expression
                return {};
            }
        }
        else if (identifier_type == Identifier::Type::table)
        {
            auto table = identifier->getTable();
            auto database = identifier->getDatabase();
            auto resolved_path = table->resolvePath(path_to_resolve);

            if (resolved_path.normalized_path.empty())
                return {};

            auto resolved_identifier = Identifier::createUnresolved();
            resolved_identifier->resolveAsColumn(resolved_path.normalized_path, resolved_path.identifier_type, table, database);

            return resolved_identifier;
        }
        else if (identifier_type == Identifier::Type::alias)
        {
            auto alias_identifier = identifier->getAliasIdentifier();

            if (path_to_resolve[0] != path[0])
                return {};

            /// If alias recursively points to constant return it is resolved to that constant
            if (alias_identifier->getExpression() && path_to_resolve.size() == 1)
                return alias_identifier->getExpression();

            if (alias_identifier->getExpression())
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Alias to non expressions are not yet supported");

            /** If alias points to other entity that can resolve path.
              * We replace start of identifier_path that matched alias, with
              * name of entity that alias points to.
              *
              * For example
              * SELECT test_table_alias.a FROM test_table as test_table_alias.
              * Actual resolve will look like test_table_alias.a -> test_table.a
              *
              * SELECT a.nested_column AS b, b.nested_column FROM test_table
              * Actual resolve will look like b.nested_column -> a.nested_column.nested_column
              */
            IdentifierPath resolve_path = alias_identifier->getPath();

            for (size_t i = 1; i < path_to_resolve.size(); ++i)
                resolve_path.emplace_back(path_to_resolve[i]);

            auto result = resolvePathWithIdentifier(resolve_path, alias_identifier);

            std::cerr << "resolvePathWithIdentifier " << toString(path_to_resolve) << " resolve path " << toString(resolve_path);
            std::cerr << " result " << result << std::endl;

            return result;
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported method");
        }
    }

    String alias_name;

    std::vector<IdentifierPtr> table_identifiers;

    std::vector<ScopePtr> inner_scopes;

    std::vector<IdentifierPtr> identifiers;

    std::vector<IdentifierPtr> unresolved_identifiers;

    std::unordered_map<ASTSelectQuery::Expression, std::vector<ExpressionPtr>> query_expression_type_to_expressions;

    std::unordered_map<std::string, IdentifierPtr> alias_name_to_alias_identifier;

    enum class ResolveStatus
    {
        unresolved,
        resolved,
        in_resolve_process
    };

    std::unordered_map<Identifier *, ResolveStatus> identifier_to_resolve_status;

};

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
