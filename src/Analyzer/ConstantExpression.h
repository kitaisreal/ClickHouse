#pragma once

#include <Analyzer/IExpression.h>

#include <Core/Field.h>

namespace DB
{

class ConstantExpression : public IExpression, public shared_ptr_helper<ConstantExpression>
{
    friend struct shared_ptr_helper<ConstantExpression>;
public:

    ExpressionType getExpressionType() const override
    {
        return ExpressionType::constant;
    }

    DataTypePtr getDataType() const override
    {
        return nullptr;
    }

    String dump() const override
    {
        return value.dump();
    }

    ExpressionPtr tryResolve(const IdentifierPath &) override
    {
        return nullptr;
    }

    const Field & getConstant() const
    {
        return value;
    }

private:
    explicit ConstantExpression(Field value_)
        : value(std::move(value_))
    {
    }

    Field value;
};

class ConstantExpression;
using ConstantExpressionPtr = std::shared_ptr<const ConstantExpression>;

}
