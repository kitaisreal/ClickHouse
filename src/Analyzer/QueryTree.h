#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Analyzer/IdentifierPath.h>
#include <Analyzer/IScope.h>
#include <Analyzer/IExpression.h>
#include <Analyzer/Identifier.h>
#include <Analyzer/ExpressionPlaceholder.h>


namespace DB
{

class QueryTree : public IScope
{
public:
    explicit QueryTree(const std::string & alias_name_)
        : alias_name(alias_name_)
    {}

    ScopeType getScopeType() const override
    {
        return ScopeType::query;
    }

    void addInnerScope(ScopePtr scope);

    void addConstant(Field constant_value, const String & alias, ASTSelectQuery::Expression query_expression_part);

    void addIdentifier(const IdentifierPath & path, const String & alias, ASTSelectQuery::Expression query_expression_part);

    void addTableExpression(TableCatalogPtr database, TablePtr table, const String & alias);

    void resolveIdentifiers() override;

    std::string dump() const;

private:

    ExpressionPtr tryResolveIdentifierFromAliases(const IdentifierPath & unresolved_identifier_path);

    ExpressionPtr tryResolveIdentifierFromTables(const IdentifierPath & unresolved_identifier_path);

    ExpressionPtr tryResolveIdentifierFromParentScope(const IdentifierPath & unresolved_identifier_path);

    void resolveIdentifier(const IdentifierPath & unresolved_identifier_path);

    IdentifierPtr addAliasForIdentifierIfNeeded(IdentifierPtr identifier, const String & alias);

    static ExpressionPtr resolvePathWithIdentifier(const IdentifierPath & path_to_resolve, const IdentifierPtr & identifier);

    String alias_name;

    std::vector<IdentifierPtr> table_identifiers;

    std::vector<ScopePtr> inner_scopes;

    std::vector<IdentifierPath> unresolved_identifiers;

    std::unordered_map<std::string, ExpressionPtr> alias_name_to_expression;

    std::unordered_map<std::string, IdentifierPath> alias_name_to_identifier_path;

    enum class ResolveStatus
    {
        unresolved,
        resolved,
        in_resolve_process
    };

    struct IdentifierResolveStatus
    {
        ResolveStatus status;
        ExpressionPtr expression;
    };

    std::unordered_map<ExpressionPlaceholder *, ResolveStatus> expression_to_resolve_status;

    std::unordered_map<std::string, IdentifierResolveStatus> identifier_path_to_resolve_status;

};

using QueryTreePtr = std::shared_ptr<QueryTree>;

}
