#pragma once

#include <string>

#include <Analyzer/ITableCatalog.h>

namespace DB
{

class IDatabaseCatalog
{
public:

    virtual ~IDatabaseCatalog() {}

    virtual bool hasDatabase(const std::string & database_name) const = 0;

    virtual TableCatalogPtr getDatabase(const std::string & database_name) const = 0;

};

using DatabaseCatalogPtr = std::shared_ptr<IDatabaseCatalog>;

}
