#pragma once

#include <vector>
#include <string>


namespace DB
{

using IdentifierPath = std::vector<std::string>;

/// Create identifier path from string with format a.b.c
IdentifierPath createIdentifierPath(const std::string & value);

/// Dump identifier path to string with format a.b.c
std::string toString(const IdentifierPath & path);

}
