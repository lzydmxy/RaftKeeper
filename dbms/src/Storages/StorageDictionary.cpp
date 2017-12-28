#include <sstream>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Parsers/ASTLiteral.h>
#include <common/logger_useful.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StorageDictionary::StorageDictionary(
    const String & table_name_,
    const NamesAndTypesList & columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    const DictionaryStructure & dictionary_structure_,
    const String & dictionary_name_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_}, table_name(table_name_),
    columns(columns_), dictionary_name(dictionary_name_),
    logger(&Poco::Logger::get("StorageDictionary"))
{
    checkNamesAndTypesCompatibleWithDictionary(dictionary_structure_);
}

BlockInputStreams StorageDictionary::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned /*threads*/)
{
    processed_stage = QueryProcessingStage::FetchColumns;
    auto dictionary = context.getExternalDictionaries().getDictionary(dictionary_name);
    return BlockInputStreams{dictionary->getBlockInputStream(column_names, max_block_size)};
}

NamesAndTypesList StorageDictionary::getNamesAndTypes(const DictionaryStructure & dictionary_structure)
{
    NamesAndTypesList dictionary_names_and_types;

    if (dictionary_structure.id)
        dictionary_names_and_types.emplace_back(dictionary_structure.id->name, std::make_shared<DataTypeUInt64>());
    if (dictionary_structure.range_min)
        dictionary_names_and_types.emplace_back(dictionary_structure.range_min->name, std::make_shared<DataTypeDate>());
    if (dictionary_structure.range_max)
        dictionary_names_and_types.emplace_back(dictionary_structure.range_max->name, std::make_shared<DataTypeDate>());
    if (dictionary_structure.key)
        for (const auto & attribute : *dictionary_structure.key)
            dictionary_names_and_types.emplace_back(attribute.name, attribute.type);

    for (const auto & attribute : dictionary_structure.attributes)
        dictionary_names_and_types.emplace_back(attribute.name, attribute.type);

    return dictionary_names_and_types;
}

void StorageDictionary::checkNamesAndTypesCompatibleWithDictionary(const DictionaryStructure & dictionary_structure) const
{
    auto dictionary_names_and_types = getNamesAndTypes(dictionary_structure);
    std::set<NameAndTypePair> namesAndTypesSet(dictionary_names_and_types.begin(), dictionary_names_and_types.end());

    for (auto & column : columns)
    {
        if (namesAndTypesSet.find(column) == namesAndTypesSet.end())
        {
            std::string message = "Not found column ";
            message += column.name + " " + column.type->getName();
            message += " in dictionary " + dictionary_name + ". ";
            message += "There are only columns ";
            message += generateNamesAndTypesDescription(dictionary_names_and_types.begin(), dictionary_names_and_types.end());
            throw Exception(message);
        }
    }
}


void registerStorageDictionary(StorageFactory & factory)
{
    factory.registerStorage("Dictionary", [](
        ASTs & args,
        const String &,
        const String & table_name,
        const String &,
        Context &,
        Context & context,
        const NamesAndTypesList & columns,
        const NamesAndTypesList & materialized_columns,
        const NamesAndTypesList & alias_columns,
        const ColumnDefaults & column_defaults,
        bool,
        bool)
    {
        if (args.size() != 1)
            throw Exception("Storage Dictionary requires single parameter: name of dictionary",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String dictionary_name = typeid_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();

        const auto & dictionary = context.getExternalDictionaries().getDictionary(dictionary_name);
        const DictionaryStructure & dictionary_structure = dictionary->getStructure();

        return StorageDictionary::create(
            table_name, columns, materialized_columns, alias_columns,
            column_defaults, dictionary_structure, dictionary_name);
    });
}

}
