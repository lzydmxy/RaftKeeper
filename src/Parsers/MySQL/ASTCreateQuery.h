#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>

namespace DB
{

namespace MySQLParser
{

class ASTCreateQuery : public IAST
{
public:
    bool temporary{false};
    bool if_not_exists{false};

    String table;
    String database;
    ASTPtr like_table;
    ASTPtr columns_list;
    ASTPtr table_options;
    ASTPtr partition_options;

    ASTPtr clone() const override;

    String getID(char) const override { return "create query"; }
};

class ParserCreateQuery : public IParserBase
{
protected:
    const char * getName() const override { return "create query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
