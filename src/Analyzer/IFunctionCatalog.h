#pragma once

#include <string>


namespace DB
{

class IFunctionCatalog
{
public:

    virtual ~IFunctionCatalog() {}

    bool has(const std::string & function_name) const;

};

using FunctionCatalogPtr = std::shared_ptr<const IFunctionCatalog>;

}
