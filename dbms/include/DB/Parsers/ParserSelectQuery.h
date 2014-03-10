#pragma once

#include <DB/Parsers/IParserBase.h>


namespace DB
{


class ParserSelectQuery : public IParserBase
{
protected:
	const char * getName() const { return "SELECT query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected);
};

}
