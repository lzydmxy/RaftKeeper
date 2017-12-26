#pragma once

#include <Storages/IStorage.h>


namespace Poco
{
    namespace Data
    {
        class SessionPool;
    }
}


namespace DB
{

/** Implements storage in the ODBC database.
  * Use ENGINE = odbc(connection_string, table_name)
  * Example ENGINE = odbc('dsn=test', table)
  * Read only.
  */
class StorageODBC : public IStorage
{
public:
    StorageODBC(
        const std::string & name,
        const std::string & connection_string,
        const std::string & remote_database_name,
        const std::string & remote_table_name,
        const NamesAndTypesList & columns);

    std::string getName() const override { return "ODBC"; }
    std::string getTableName() const override { return name; }
    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    std::string name;

    std::string connection_string;
    std::string remote_database_name;
    std::string remote_table_name;

    NamesAndTypesList columns;

    std::shared_ptr<Poco::Data::SessionPool> pool;
};
}
