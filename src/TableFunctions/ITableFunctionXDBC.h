#pragma once

#include <Storages/StorageXDBC.h>
#include <TableFunctions/ITableFunction.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Bridge/XDBCBridgeHelper.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

namespace DB
{
/**
 * Base class for table functions, that works over external bridge
 * Xdbc (Xdbc connect string, table) - creates a temporary StorageXDBC.
 */
class ITableFunctionXDBC : public ITableFunction
{
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name, ColumnsDescription cached_columns) const override;

    /* A factory method to create bridge helper, that will assist in remote interaction */
    virtual BridgeHelperPtr createBridgeHelper(Context & context,
        const Poco::Timespan & http_timeout_,
        const std::string & connection_string_) const = 0;

    ColumnsDescription getActualTableStructure(const Context & context) const override;

    void parseArguments(const ASTPtr & ast_function, const Context & context) override;

    void startBridgeIfNot(const Context & context) const;

    String connection_string;
    String schema_name;
    String remote_table_name;
    mutable BridgeHelperPtr helper;
};

class TableFunctionJDBC : public ITableFunctionXDBC
{
public:
    static constexpr auto name = "jdbc";
    std::string getName() const override
    {
        return name;
    }

private:
    BridgeHelperPtr createBridgeHelper(Context & context,
        const Poco::Timespan & http_timeout_,
        const std::string & connection_string_) const override
    {
        return std::make_shared<XDBCBridgeHelper<JDBCBridgeMixin>>(context, http_timeout_, connection_string_);
    }

    const char * getStorageTypeName() const override { return "JDBC"; }
};

class TableFunctionODBC : public ITableFunctionXDBC
{
public:
    static constexpr auto name = "odbc";
    std::string getName() const override
    {
        return name;
    }

private:
    BridgeHelperPtr createBridgeHelper(Context & context,
        const Poco::Timespan & http_timeout_,
        const std::string & connection_string_) const override
    {
        return std::make_shared<XDBCBridgeHelper<ODBCBridgeMixin>>(context, http_timeout_, connection_string_);
    }

    const char * getStorageTypeName() const override { return "ODBC"; }
};
}
