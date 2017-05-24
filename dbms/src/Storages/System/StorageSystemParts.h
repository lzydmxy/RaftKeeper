#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements system table 'parts' which allows to get information about data parts for tables of MergeTree family.
  */
class StorageSystemParts : private ext::shared_ptr_helper<StorageSystemParts>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemParts>;
public:
    static StoragePtr create(const std::string & name_);

    std::string getName() const override { return "SystemParts"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

    BlockInputStreams read(
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

private:
    const std::string name;
    NamesAndTypesList columns;

    StorageSystemParts(const std::string & name_);
};

}
