#pragma once

#include <string>

#include <Analyzer/ITable.h>


namespace DB
{

class ITableCatalog
{
public:

    virtual ~ITableCatalog() {}

    virtual std::string getName() const = 0;

    virtual bool hasTable(const std::string & table) const = 0;

    virtual TablePtr getTable(const std::string & table) const = 0;

};

using TableCatalogPtr = std::shared_ptr<const ITableCatalog>;

}
