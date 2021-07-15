#pragma once

#include <memory>
#include <string>

#include <Common/TypePromotion.h>
#include <DataTypes/IDataType.h>

#include <Analyzer/IdentifierPath.h>

namespace DB
{

/// TODO: Add function
enum class ExpressionType
{
    constant,
    identifier
};

class IExpression;
using ExpressionPtr = std::shared_ptr<IExpression>;

class IExpression : public TypePromotion<IExpression>
{
public:

    virtual ~IExpression() {}

    virtual ExpressionType getExpressionType() const = 0;

    virtual DataTypePtr getDataType() const = 0;

    virtual void resolveType() = 0;

    virtual bool isResolved() const = 0;

    virtual ExpressionPtr tryResolve(const IdentifierPath & path_to_resolve);

    virtual String dump() const = 0;

};

}
