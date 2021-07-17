#include <Analyzer/QueryTree.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Analyzer/ConstantExpression.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_IDENTIFIER;
}

void QueryTree::addInnerScope(ScopePtr scope)
{
    assert(scope->parent_scope != nullptr);
    scope->parent_scope = this;
    inner_scopes.emplace_back(std::move(scope));
}

void QueryTree::addConstant(Field constant_value, const String & alias, ASTSelectQuery::Expression query_expression_part)
{
    (void)(query_expression_part);
    auto constant_expression = ConstantExpression::create(std::move(constant_value));
    alias_name_to_expression[alias] = constant_expression;
    // auto & expressions = query_expression_type_to_expressions[query_expression_part];

    // if (!alias.empty())
    // {
    //     auto alias_identifer = Identifier::createAlias(constant_expression, alias);
    //     alias_name_to_alias_identifier[alias] = alias_identifer;
    //     expressions.emplace_back(std::move(alias_identifer));
    // }
    // else
    // {
    //     expressions.emplace_back(std::move(constant_expression));
    // }
}

void QueryTree::addIdentifier(const IdentifierPath & path, const String & alias, ASTSelectQuery::Expression query_expression_part)
{
    (void)(query_expression_part);
    // auto unresolved_identifier = Identifier::createUnresolved(path);
    // unresolved_identifiers.emplace_back(unresolved_identifier);
    alias_name_to_identifier_path[alias] = path;
    unresolved_identifiers.emplace_back(path);

    // auto & expressions = query_expression_type_to_expressions[query_expression_part];

    // if (!alias.empty())
    // {
    //     auto alias_identifer = Identifier::createAlias(unresolved_identifier, alias);
    //     alias_name_to_alias_identifier[alias] = alias_identifer;
    //     expressions.emplace_back(std::move(alias_identifer));
    // }
    // else
    // {
    //     expressions.emplace_back(std::move(unresolved_identifier));
    // }
}

void QueryTree::addTableExpression(TableCatalogPtr database, TablePtr table, const String & alias)
{
    std::cerr << "Scope::addTableExpression " << table->getDatabaseName() << " table name " << table->getTableName();
    std::cerr << " alias " << alias << std::endl;

    auto table_identifier = Identifier::createForTable({database->getName(), table->getTableName()}, table, database);
    table_identifiers.emplace_back(table_identifier);

    if (!alias.empty())
    {
        auto alias_identifier = Identifier::createAlias(table_identifier, alias);
        table_identifiers.emplace_back(alias_identifier);
    }
}

ExpressionPtr QueryTree::tryResolveExpressionFromAliases(const IdentifierPath & path)
{
    std::cerr << "Scope::tryResolveIdentifierFromAliases " << this << " path " << toString(path) << std::endl;

    auto expression_it = alias_name_to_expression.find(path[0]);
    if (expression_it != alias_name_to_expression.end())
    {
        /// Resolve path from expression;
        return nullptr;
    }

    auto identifier_it = alias_name_to_identifier_path.find(path[0]);


    // auto alias_expression_placeholder = it->second;

    // ExpressionPtr result;

    // if (alias_expression_placeholder->isResolved())
    // {
    //     auto resolved_expression = alias_expression_placeholder->getExpression();
    //     std::cerr << "Scope::tryResolveIdentifierFromAliases alias resolved " << resolved_expression->dump() << std::endl;
    //     result = resolved_expression->tryResolve(path);
    // }
    // else
    // {
    //     std::cerr << "Scope::tryResolveIdentifierFromAliases alias not resolved start " << toString(alias_identifier->getPath())
    //               << std::endl;
    //     resolve(alias_);
    //     std::cerr << "Scope::tryResolveIdentifierFromAliases alias not resolved finished "
    //               << toString(alias_identifier->getAliasIdentifier()->getPath()) << std::endl;
    //     result = resolvePathWithIdentifier(path, alias_identifier);
    // }

    // std::cerr << "Scope::tryResolveIdentifierFromAliases finished " << result << std::endl;

    return result;
}

ExpressionPtr QueryTree::tryResolveExpressionFromTables(const IdentifierPath & path)
{
    std::cerr << "Scope::tryResolveExpressionFromTables " << this << " path " << toString(path) << std::endl;

    ExpressionPtr result_expression;

    for (auto & table_identifier : table_identifiers)
    {
        auto resolved_identifier = table_identifier->tryResolve(path);

        if (resolved_identifier && result_expression)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Ambigious resolve identifier {}", toString(path));

        result_expression = resolved_identifier;
    }

    std::cerr << "Scope::tryResolveExpressionFromTables finished " << result_expression->dump() << std::endl;
    return result_expression;
}

ExpressionPtr QueryTree::tryResolveExpressionFromParentScope(const IdentifierPath & path)
{
    IScope * parent_scope_to_check = parent_scope;

    while (parent_scope_to_check)
    {
        auto resolved_expression = parent_scope_to_check->tryResolveExpression(path);
        if (resolved_expression)
            return resolved_expression;
    }

    return nullptr;
}

void QueryTree::resolveIdentifier(const IdentifierPath & unresolved_identifier_path)
{
    std::string unresolved_identifier_path_concatenated = toString(unresolved_identifier_path);
    std::cerr << "Scope::resolveIdentifier start " << unresolved_identifier_path_concatenated << std::endl;

    auto it = identifier_path_to_resolve_status.find(unresolved_identifier_path_concatenated);
    if (it->second.status == ResolveStatus::in_resolve_process)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Cyclic aliases for identifier {}", unresolved_identifier_path_concatenated);

    it->second.status = ResolveStatus::in_resolve_process;

    auto resolved_from_aliases = tryResolveIdentifierFromAliases(unresolved_identifier_path);
    if (resolved_from_aliases)
    {
        it->second.status = ResolveStatus::resolved;
        it->second = resolved_from_aliases;
        return;
    }

    if (!unresolved_identifier->isResolved())
    {
        auto resolved_from_tables = tryResolveIdentifierFromTables(unresolved_identifier_path);
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
        // identifiers.emplace_back(unresolved_identifier);
        identifier_to_resolve_status[unresolved_identifier.get()] = ResolveStatus::resolved;
    }
}

void QueryTree::resolveIdentifiers()
{
    for (auto & unresolved_identifier : unresolved_identifiers)
        identifier_to_resolve_status[unresolved_identifier.get()] = {ResolveStatus::unresolved, nullptr};

    for (const auto & inner_scope : inner_scopes)
        inner_scope->resolveIdentifiers();

    while (!unresolved_identifiers.empty())
    {
        auto unresolved_identifier_path = unresolved_identifiers.back();
        unresolved_identifiers.pop_back();

        std::string unresolved_identifier_path_concatenated = toString(unresolved_identifier_path);

        /// Identifier potentially can be resolved during inner scope resolveIdentifiers call
        /// or it can be resolved recursively during resolveIdentifier call
        auto resolve_status_it = identifier_to_resolve_status.find(unresolved_identifier_path_concatenated);
        if (resolve_status_it->second.status == ResolveStatus::resolved)
            continue;

        resolveIdentifier(unresolved_identifier);
    }
}

std::string QueryTree::dump() const
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


IdentifierPtr QueryTree::addAliasForIdentifierIfNeeded(IdentifierPtr identifier, const String & alias)
{
    if (alias.empty())
        return nullptr;

    auto alias_identifer = Identifier::createAlias(identifier, alias);
    alias_name_to_alias_identifier[alias] = alias_identifer;

    return alias_identifer;
}

ExpressionPtr QueryTree::resolvePathWithIdentifier(const IdentifierPath & path_to_resolve, const IdentifierPtr & identifier)
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

}
