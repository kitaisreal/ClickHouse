#include <Analyzer/IdentifierPath.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

IdentifierPath createIdentifierPath(const std::string & value)
{
    IdentifierPath path;
    size_t previous_dot_index = 0;

    for (size_t i = 0; i < value.size(); ++i)
    {
        if (value[i] == '.')
        {
            path.emplace_back(value, previous_dot_index, i);
            previous_dot_index = i + 1;
        }
    }

    path.emplace_back(value, previous_dot_index, value.size());

    return path;
}

std::string toString(const IdentifierPath & path)
{
    WriteBufferFromOwnString buffer;

    for (const auto & path_part : path)
    {
        buffer << path_part;
        buffer << '.';
    }

    if (!path.empty())
        --buffer.position();

    return buffer.str();
}

}
