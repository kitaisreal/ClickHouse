#include <gtest/gtest.h>

#include <DataTypes/DataTypesNumber.h>

#include <Parsers/TokenIterator.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/IdentifierResolver.h>

using namespace DB;

struct MockColumn
{
    std::string name;
    DataTypePtr type;
};

class MockTable : public ITable, public std::enable_shared_from_this<MockTable>
{
public:

    explicit MockTable(const std::string & name_, const std::vector<MockColumn> & columns)
        : name(name_)
    {
        for (const auto & column : columns)
            column_name_to_type[column.name] = column.type;
    }

    const std::string & getName() const
    {
        return name;
    }

    std::vector<IdentifierPtr> resolveIdentifiers(const IdentifierPath & path) const override
    {
        auto column_name = path[0];

        auto it = column_name_to_type.find(column_name);

        if (it == column_name_to_type.end())
            return {};

        auto result_identifiers = { Identifier::createForColumn(path, it->second, shared_from_this()) };
        return result_identifiers;
    }
private:
    std::string name;

    std::unordered_map<std::string, DataTypePtr> column_name_to_type;

};

class MockDatabase : public IDatabaseUpdated
{
public:

    explicit MockDatabase(const std::string name_, const std::vector<std::shared_ptr<MockTable>> & tables)
        : name(name_)
    {
        for (const auto & table : tables)
            table_name_to_table[table->getName()] = table;
    }

    const std::string & getName() const
    {
        return name;
    }

    bool hasTable(const String & table) const override
    {
        return table_name_to_table.find(table) != table_name_to_table.end();
    }

    TablePtr getTable(const String & table) const override
    {
        return table_name_to_table.find(table)->second;
    }

private:

    std::string name;

    std::unordered_map<std::string, TablePtr> table_name_to_table;

};

class MockDatabaseCatalog : public IDatabaseCatalog
{
public:
    const std::string & getName() const
    {
        return name;
    }

    bool hasDatabase(const std::string & database_name) const override
    {
        return database_name_to_database.find(database_name) != database_name_to_database.end();
    }

    DatabaseUpdatedPtr getDatabase(const std::string & database_name) const override
    {
        return database_name_to_database.find(database_name)->second;
    }

    void registerDatabase(std::shared_ptr<MockDatabase> database)
    {
        std::string database_name = database->getName();
        database_name_to_database[database_name] = database;
    }

private:

    std::string name;

    std::unordered_map<std::string, DatabaseUpdatedPtr> database_name_to_database;

};

class MockFunctionCatalog : public IFunctionCatalog
{

};


static void IDENTIFIER_EQ(const IdentifierPtr & identifier, const IdentifierPath & path, Identifier::Type type, const DataTypePtr & data_type = {})
{
    EXPECT_EQ(identifier->getPath(), path);
    EXPECT_EQ(identifier->getType(), type);

    if (data_type)
        EXPECT_EQ(identifier->getDataType()->getName(), data_type->getName());
}

static ScopePtr topScopeForQuery(const String & query, std::shared_ptr<IDatabaseCatalog> database_catalog)
{
    ParserSelectWithUnionQuery parser;
    ASTPtr ast = parseQuery(parser, query, 1000, 1000);

    IdentifierResolver resolver(ast, database_catalog, nullptr);
    return resolver.getScopes().back();
}

TEST(IdentifierResolver, TableIdentifier)
{
    std::shared_ptr<MockTable> table = std::make_shared<MockTable>("table", std::vector<MockColumn>{{"column", std::make_shared<DataTypeUInt8>()}});
    std::shared_ptr<MockDatabase> database = std::make_shared<MockDatabase>("default", std::vector<std::shared_ptr<MockTable>>{table});

    std::shared_ptr<MockDatabaseCatalog> database_catalog = std::make_shared<MockDatabaseCatalog>();
    database_catalog->registerDatabase(database);

    // {
    //     auto top_scope = topScopeForQuery("SELECT column FROM table", database_catalog);
    //     IDENTIFIER_EQ(top_scope->resolveIdentifier("default.table"), {"default", "table"}, Identifier::Type::table);
    //     IDENTIFIER_EQ(top_scope->resolveIdentifier("column"), {"column"}, Identifier::Type::column, std::make_shared<DataTypeUInt8>());
    // }
    // {
    //     auto top_scope = topScopeForQuery("SELECT column FROM default.table", database_catalog);
    //     IDENTIFIER_EQ(top_scope->resolveIdentifier("default.table"), {"default", "table"}, Identifier::Type::table);
    //     IDENTIFIER_EQ(top_scope->resolveIdentifier("column"), {"column"}, Identifier::Type::column, std::make_shared<DataTypeUInt8>());
    // }
    {
        auto top_scope = topScopeForQuery("SELECT table.column FROM default.table", database_catalog);
        IDENTIFIER_EQ(top_scope->resolveIdentifier("default.table"), {"default", "table"}, Identifier::Type::table);
        IDENTIFIER_EQ(top_scope->resolveIdentifier("column"), {"column"}, Identifier::Type::column, std::make_shared<DataTypeUInt8>());
    }
    // {
    //     auto top_scope = topScopeForQuery("SELECT table.column FROM default.table", database_catalog);
    //     IDENTIFIER_EQ(top_scope->resolveIdentifier("default.table"), {"default", "table"}, Identifier::Type::table);
    //     IDENTIFIER_EQ(top_scope->resolveIdentifier("column"), {"column"}, Identifier::Type::column, std::make_shared<DataTypeUInt8>());
    // }
    // {
    //     auto top_scope = topScopeForQuery("SELECT t.column FROM default.table AS t", database_catalog);
    //     IDENTIFIER_EQ(top_scope->resolveIdentifier("t"), {"t"}, Identifier::Type::alias);
    //     IDENTIFIER_EQ(top_scope->resolveIdentifier("default.table"), {"default", "table"}, Identifier::Type::table);
    //     IDENTIFIER_EQ(top_scope->resolveIdentifier("column"), {"column"}, Identifier::Type::column, std::make_shared<DataTypeUInt8>());
    // }
}
