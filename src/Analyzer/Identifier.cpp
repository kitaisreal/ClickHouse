#include <Analyzer/Identifier.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

const char * Identifier::typeToString(Type identifier_type)
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


IdentifierPtr Identifier::createUnresolved()
{
    auto identifier = std::make_shared<Identifier>();
    return identifier;
}

IdentifierPtr Identifier::createUnresolved(const IdentifierPath & path_)
{
    auto identifier = std::make_shared<Identifier>();

    identifier->path = path_;

    return identifier;
}

IdentifierPtr Identifier::createAlias(ExpressionPtr expression, const String & alias_name)
{
    auto identifier = createUnresolved();

    identifier->identifier_type = Type::alias;
    identifier->path = {alias_name};
    identifier->expression = expression;
    identifier->resolved = false;

    return identifier;
}

IdentifierPtr Identifier::createForTable(const IdentifierPath & path_, TablePtr table_, TableCatalogPtr database_)
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

void Identifier::resolveAsColumn(const IdentifierPath & path_, DataTypePtr data_type_, TablePtr table_, TableCatalogPtr database_)
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

void Identifier::resolveAsIdentifier(const IdentifierPtr & identifier)
{
    assert(resolved == false);

    *this = *identifier;
}

void Identifier::resolveAsLambdaArgument(String & argument_name)
{
    assert(resolved == false);

    identifier_type = Type::lambda_parameter;
    path = {argument_name};
    resolved = true;
}

ExpressionPtr Identifier::tryResolve(const IdentifierPath & path_to_resolve)
{
    auto identifier_type = getIdentifierType();

    if (identifier_type == Type::lambda_parameter)
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported method");
    }
    else if (identifier_type == Type::column)
    {
        if (table)
        {
            /// If column is from table
            auto resolved_path_result = table->resolvePath(path_to_resolve);

            if (resolved_path_result.normalized_path.empty())
                return {};

            auto identifier = Identifier::createUnresolved();
            identifier->resolveAsColumn(resolved_path_result.normalized_path, resolved_path_result.identifier_type, table, database);

            return identifier;
        }
        else if (expression)
        {

            /// If column is from expression
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported method");
        }
    }
    else if (identifier_type == Type::table)
    {
        auto resolved_path_result = table->resolvePath(path_to_resolve);

        if (resolved_path_result.normalized_path.empty())
            return {};

        auto identifier = Identifier::createUnresolved();
        identifier->resolveAsColumn(resolved_path_result.normalized_path, resolved_path_result.identifier_type, table, database);

        return identifier;
    }
    else if (identifier_type == Type::alias)
    {
        if (path_to_resolve[0] != path[0])
            return {};

        IdentifierPath updated_path;
        updated_path.reserve(path_to_resolve.size() - 1);

        for (size_t i = 1; i < path_to_resolve.size(); ++i)
            updated_path.emplace_back(path_to_resolve[i]);

        return expression->tryResolve(path_to_resolve);
    }
}

std::string Identifier::dump() const
{
    WriteBufferFromOwnString out;

    out << typeToString(identifier_type);
    out << ' ';

    if (identifier_type == Type::column || identifier_type == Type::table || identifier_type == Type::lambda_parameter)
    {
        out << toString(path);
        out << ' ';

        if (data_type)
            out << data_type->getName();
    }
    else if (identifier_type == Type::alias)
    {
        out << path[0];
        out << ' ';
        out << expression->dump();
    }

    return out.str();
}

const IExpression * Identifier::getExpressionRemoveAliasIfNeeded() const
{
    const Identifier * value = this;

    while (value->identifier_type == Identifier::Type::alias)
    {
        /// TODO: Check
        value = static_cast<const Identifier *>(value->expression.get());
    }

    return value;
}

const IExpression * Identifier::getIdentifierRemoveAliasIfNeeded() const
{
    const auto * expression = getExpressionRemoveAliasIfNeeded();
    assert(expression->getExpressionType() == ExpressionType::identifier);
    const auto * identifier = static_cast<const Identifier *>(expression);

    return identifier;
}

}
