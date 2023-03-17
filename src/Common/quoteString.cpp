#include <Common/quoteString.h>
#include <Common/IO/WriteHelpers.h>
#include <Common/IO/WriteBufferFromString.h>


namespace RK
{
String quoteString(const StringRef & x)
{
    String res(x.size, '\0');
    WriteBufferFromString wb(res);
    writeQuotedString(x, wb);
    return res;
}


String doubleQuoteString(const StringRef & x)
{
    String res(x.size, '\0');
    WriteBufferFromString wb(res);
    writeDoubleQuotedString(x, wb);
    return res;
}


String backQuote(const StringRef & x)
{
    String res(x.size, '\0');
    {
        WriteBufferFromString wb(res);
        writeBackQuotedString(x, wb);
    }
    return res;
}


String backQuoteIfNeed(const StringRef & x)
{
    String res(x.size, '\0');
    {
        WriteBufferFromString wb(res);
        writeProbablyBackQuotedString(x, wb);
    }
    return res;
}
}
