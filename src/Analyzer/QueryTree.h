#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Analyzer/IdentifierPath.h>
#include <Analyzer/IScope.h>
#include <Analyzer/IExpression.h>
#include <Analyzer/Identifier.h>


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

    ExpressionPtr tryResolveIdentifierFromAliases(const IdentifierPath & path);

    ExpressionPtr tryResolveIdentifierFromTables(const IdentifierPath & path);

    ExpressionPtr tryResolveIdentifierFromParentScope(const IdentifierPath & path);

    void resolveIdentifier(IdentifierPtr unresolved_identifier);

    void resolveIdentifiers() override;

    std::string dump() const;

private:

    IdentifierPtr addAliasForIdentifierIfNeeded(IdentifierPtr identifier, const String & alias);

    static ExpressionPtr resolvePathWithIdentifier(const IdentifierPath & path_to_resolve, const IdentifierPtr & identifier);

    String alias_name;

    std::vector<IdentifierPtr> table_identifiers;

    std::vector<ScopePtr> inner_scopes;

    std::vector<IdentifierPtr> identifiers;

    std::vector<IdentifierPtr> unresolved_identifiers;

    std::unordered_map<ASTSelectQuery::Expression, std::vector<ExpressionPtr>> query_expression_type_to_expressions;

    std::unordered_map<std::string, ExpressionPtr> alias_name_to_expression;

    enum class ResolveStatus
    {
        unresolved,
        resolved,
        in_resolve_process
    };

    std::unordered_map<Identifier *, ResolveStatus> identifier_to_resolve_status;

};

using QueryTreePtr = std::shared_ptr<QueryTree>;

}
