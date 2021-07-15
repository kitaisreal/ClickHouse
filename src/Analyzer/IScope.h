#pragma once

#include <memory>

#include <Common/TypePromotion.h>

#include <Analyzer/IdentifierPath.h>
#include <Analyzer/IExpression.h>

namespace DB
{

enum ScopeType
{
    query,
    expression
};

class IScope : public TypePromotion<IScope>
{
public:

    virtual ~IScope() {}

    virtual ScopeType getScopeType() const = 0;

    virtual void resolveIdentifiers() = 0;

    virtual ExpressionPtr tryResolveExpression(const IdentifierPath & path) = 0;

    const IScope * getParentScope() const
    {
        return parent_scope;
    }

// protected:

    IScope * parent_scope;

};

using ScopePtr = std::shared_ptr<IScope>;

}
