#pragma once

#include <Analyzer/IExpression.h>


namespace DB
{

class ExpressionPlaceholder
{
public:

    bool isResolved()
    {
        return expression != nullptr;
    }

    void resolve(ExpressionPtr expression_)
    {
        assert(expression == nullptr);
        expression = std::move(expression_);
    }

    ExpressionPtr getExpression()
    {
        return expression;
    }

private:

    ExpressionPtr expression;
};

using ExpressionPlaceholderPtr = std::shared_ptr<ExpressionPlaceholder>;

}

