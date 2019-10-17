#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/quoteString.h>

#include <common/JSON.h>

namespace DB
{

void MergeTreeDataPartTTLInfos::update(const MergeTreeDataPartTTLInfos & other_infos)
{
    for (const auto & [name, ttl_info] : other_infos.columns_ttl)
    {
        columns_ttl[name].update(ttl_info);
        updatePartMinMaxTTL(ttl_info.min, ttl_info.max);
    }

    table_ttl.update(other_infos.table_ttl);
    updatePartMinMaxTTL(table_ttl.min, table_ttl.max);

    for (const auto & [expression, ttl_info] : other_infos.moves_ttl)
    {
        moves_ttl[expression].update(ttl_info);
    }
}


void MergeTreeDataPartTTLInfos::read(ReadBuffer & in)
{
    String json_str;
    readString(json_str, in);
    assertEOF(in);

    JSON json(json_str);
    if (json.has("columns"))
    {
        JSON columns = json["columns"];
        for (auto col : columns)
        {
            MergeTreeDataPartTTLInfo ttl_info;
            ttl_info.min = col["min"].getUInt();
            ttl_info.max = col["max"].getUInt();
            String name = col["name"].getString();
            columns_ttl.emplace(name, ttl_info);

            updatePartMinMaxTTL(ttl_info.min, ttl_info.max);
        }
    }
    if (json.has("table"))
    {
        JSON table = json["table"];
        table_ttl.min = table["min"].getUInt();
        table_ttl.max = table["max"].getUInt();

        updatePartMinMaxTTL(table_ttl.min, table_ttl.max);
    }
    if (json.has("moves"))
    {
        JSON moves = json["moves"];
        for (auto move : moves)
        {
            MergeTreeDataPartTTLInfo ttl_info;
            ttl_info.min = move["min"].getUInt();
            ttl_info.max = move["max"].getUInt();
            String expression = move["expression"].getString();
            moves_ttl.emplace(expression, ttl_info);
        }
    }
}


void MergeTreeDataPartTTLInfos::write(WriteBuffer & out) const
{
    writeString("ttl format version: 1\n", out);
    writeString("{", out);
    if (!columns_ttl.empty())
    {
        writeString("\"columns\":[", out);
        for (auto it = columns_ttl.begin(); it != columns_ttl.end(); ++it)
        {
            if (it != columns_ttl.begin())
                writeString(",", out);

            writeString("{\"name\":", out);
            writeString(doubleQuoteString(it->first), out);
            writeString(",\"min\":", out);
            writeIntText(it->second.min, out);
            writeString(",\"max\":", out);
            writeIntText(it->second.max, out);
            writeString("}", out);
        }
        writeString("]", out);
    }
    if (table_ttl.min)
    {
        if (!columns_ttl.empty())
            writeString(",", out);
        writeString("\"table\":{\"min\":", out);
        writeIntText(table_ttl.min, out);
        writeString(",\"max\":", out);
        writeIntText(table_ttl.max, out);
        writeString("}", out);
    }
    if (!moves_ttl.empty())
    {
        if (!columns_ttl.empty() || table_ttl.min)
            writeString(",", out);
        writeString("\"moves\":[", out);
        for (auto it = moves_ttl.begin(); it != moves_ttl.end(); ++it)
        {
            if (it != moves_ttl.begin())
                writeString(",", out);

            writeString("{\"expression\":", out);
            writeString(doubleQuoteString(it->first), out);
            writeString(",\"min\":", out);
            writeIntText(it->second.min, out);
            writeString(",\"max\":", out);
            writeIntText(it->second.max, out);
            writeString("}", out);
        }
        writeString("]", out);
    }
    writeString("}", out);
}

}
