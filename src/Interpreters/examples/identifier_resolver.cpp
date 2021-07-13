#include <iostream>

#include <DataTypes/DataTypesNumber.h>

#include <Parsers/TokenIterator.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/IdentifierResolver.h>

using namespace DB;

struct MockColumnData
{
    std::string name;
    DataTypePtr type;
};

class MockTable : public ITable, public std::enable_shared_from_this<MockTable>
{
public:

    explicit MockTable(const StorageID & storage_id_, const std::vector<MockColumnData> & columns)
        : storage_id(storage_id_)
    {
        for (const auto & column : columns)
            column_name_to_type[column.name] = column.type;
    }

    StorageID getStorageID() const override
    {
        return storage_id;
    }

    bool hasPath(const IdentifierPath & path) const override
    {
        auto resolved_path = resolvePath(path);
        bool has_path = !resolved_path.normalized_path.empty();

        return has_path;
    }

    IdentifierResolvedResult resolvePath(const IdentifierPath & path) const override
    {
        /// TODO: Add support for complex types

        auto database_name = storage_id.getDatabaseName();
        auto table_name = storage_id.getTableName();

        const auto & path_start = path[0];
        if (auto type = columnDataTypeFor(path_start))
            return {normalizePath({path_start}), type};

        if (path.size() == 1)
            return {};

        if (path_start == table_name)
        {
            auto column_name = path[1];

            if (auto type = columnDataTypeFor(column_name))
                return {normalizePath({path_start}), type};
        }

        if (path.size() == 2)
            return {};

        if (path_start == database_name && path[1] == table_name)
        {
            auto column_name = path[2];

            if (auto type = columnDataTypeFor(column_name))
                return {normalizePath({column_name}), type};
        }

        return {};
    }

private:

    DataTypePtr columnDataTypeFor(const String & column_name) const
    {
        auto it = column_name_to_type.find(column_name);

        if (it == column_name_to_type.end())
            return {};

        return it->second;
    }

    IdentifierPath normalizePath(const IdentifierPath & column_name_path) const
    {
        IdentifierPath normalized_path = { storage_id.getDatabaseName(), storage_id.getTableName() };

        normalized_path.insert(normalized_path.end(), column_name_path.begin(), column_name_path.end());

        return normalized_path;
    }

    StorageID storage_id;

    std::unordered_map<std::string, DataTypePtr> column_name_to_type;

};

struct MockTableData
{
    String table_name;
    std::vector<MockColumnData> column_data;
};

class MockDatabase : public IDatabaseUpdated
{
public:

    explicit MockDatabase(const std::string name_, const std::vector<MockTableData> & tables)
        : name(name_)
    {
        for (const auto & table : tables)
        {
            StorageID storage_id(name, table.table_name);
            table_name_to_table[table.table_name] = std::make_shared<MockTable>(storage_id, table.column_data);
        }
    }

    String getName() const override
    {
        return name;
    }

    bool hasTable(const String & table) const override
    {
        return table_name_to_table.find(table) != table_name_to_table.end();
    }

    TablePtr getTable(const String & table) const override
    {
        auto it = table_name_to_table.find(table);
        if (it == table_name_to_table.end())
            return {};

        return it->second;
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

int main(int argc, char ** argv)
{
    (void)(argc);
    (void)(argv);

    std::shared_ptr<MockDatabaseCatalog> catalog = std::make_shared<MockDatabaseCatalog>();

    MockTableData test_table
    {
        .table_name = "test_table",
        .column_data = {{"test_column_1", std::make_shared<DataTypeUInt8>()}}
    };

    std::shared_ptr<MockDatabase> database = std::make_shared<MockDatabase>("default", std::vector<MockTableData>{test_table});

    catalog->registerDatabase(database);

    std::string query = "SELECT 1 AS a, a as b, b as c, c FROM test_table";

    ParserSelectWithUnionQuery parser;
    ASTPtr ast = parseQuery(parser, query, 1000, 1000);
    std::string result = ast->dumpTree(0);

    std::cerr << "AST dump tree " << result << std::endl;

    IdentifierResolver::Settings settings
    {
        .default_database_name = "default",
        .prefer_column_name_to_alias = false
    };

    IdentifierResolver resolver(ast, catalog, nullptr, settings);

    const auto & scopes = resolver.getScopes();
    auto outer_scope = scopes[0];
    std::cerr << "Scope dump " << std::endl;
    std::cerr << outer_scope->dump() << std::endl;

    return 0;
}
