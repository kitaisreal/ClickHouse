#pragma once

#include <Analyzer/IExpression.h>
#include <Analyzer/ITable.h>
#include <Analyzer/ITableCatalog.h>
#include <Analyzer/IdentifierPath.h>


namespace DB
{

class Identifier;
using IdentifierPtr = std::shared_ptr<Identifier>;

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

    static const char * typeToString(Type identifier_type);

    static IdentifierPtr createUnresolved();

    static IdentifierPtr createUnresolved(const IdentifierPath & path_);

    static IdentifierPtr createAlias(ExpressionPtr expression, const String & alias_name);

    static IdentifierPtr createForTable(const IdentifierPath & path_, TablePtr table_, TableCatalogPtr database_);

    void resolveAsColumn(const IdentifierPath & path_, DataTypePtr data_type_, TablePtr table_, TableCatalogPtr database_);

    void resolveAsIdentifier(const IdentifierPtr & identifier);

    void resolveAsLambdaArgument(String & argument_name);

    ExpressionType getExpressionType() const override
    {
        return ExpressionType::identifier;
    }

    DataTypePtr getDataType() const override
    {
        const auto * expression = getExpressionRemoveAliasIfNeeded();
        return expression->getDataType();
    }

    Type getIdentifierType() const
    {
        return identifier_type;
    }

    bool isResolved() const override
    {
        const auto * expression = getExpressionRemoveAliasIfNeeded();
        return expression->isResolved();
    }

    ExpressionPtr tryResolve(const IdentifierPath &) override;

    ExpressionPtr getExpression() const
    {
        assert(identifier_type == Type::alias);
        return expression;
    }

    const IdentifierPath & getPath() const { return path; }

    TablePtr getTable() const
    {
        return getIdentifierRemoveAliasIfNeeded()->table;
    }

    TableCatalogPtr getDatabase() const
    {
        return getIdentifierRemoveAliasIfNeeded()->database;
    }

    std::string dump() const override;

private:
    /// Type of identifier
    Type identifier_type;

    /// Valid for column, constant, expression identifier
    DataTypePtr data_type;

    /// Valid for column, table, lambda parameter identifier
    IdentifierPath path;

    /// Valid for column, table identifier
    TablePtr table;
    TableCatalogPtr database;

    /// Valid for column, alias identifier
    ExpressionPtr expression;

    /// Is resolved
    bool resolved = false;

    const IExpression * getExpressionRemoveAliasIfNeeded() const;

    const Identifier * getIdentifierRemoveAliasIfNeeded() const;

};

}
