#pragma once

#include <string>

#include <Analyzer/IdentifierPath.h>
#include <DataTypes/IDataType.h>


namespace DB
{

struct IdentifierResolvedResult
{
    IdentifierPath normalized_path;
    DataTypePtr identifier_type;
};

class ITable
{
public:

    virtual ~ITable() {}

    virtual std::string getDatabaseName() const = 0;

    virtual std::string getTableName() const = 0;

    virtual bool hasPath(const IdentifierPath & path) const = 0;

    virtual IdentifierResolvedResult resolvePath(const IdentifierPath & path) const = 0;

    /// TODO: Aliases

    /// TODO: Asterisk

};

using TablePtr = std::shared_ptr<const ITable>;

}
