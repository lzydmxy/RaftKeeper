#include <Storages/System/StorageSystemColumns.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/VirtualColumnUtils.h>
#include <Parsers/queryToString.h>
#include <Databases/IDatabase.h>


namespace DB
{

NamesAndTypesList StorageSystemColumns::getNamesAndTypes()
{
    return {
        { "database",           std::make_shared<DataTypeString>() },
        { "table",              std::make_shared<DataTypeString>() },
        { "name",               std::make_shared<DataTypeString>() },
        { "type",               std::make_shared<DataTypeString>() },
        { "default_kind",       std::make_shared<DataTypeString>() },
        { "default_expression", std::make_shared<DataTypeString>() },
        { "data_compressed_bytes",      std::make_shared<DataTypeUInt64>() },
        { "data_uncompressed_bytes",    std::make_shared<DataTypeUInt64>() },
        { "marks_bytes",                std::make_shared<DataTypeUInt64>() },
    };
}

void StorageSystemColumns::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const
{
    Block block_to_filter;

    std::map<std::pair<std::string, std::string>, StoragePtr> storages;

    {
        Databases databases = context.getDatabases();

        /// Add `database` column.
        MutableColumnPtr database_column_mut = ColumnString::create();
        for (const auto & database : databases)
        {
            if (context.hasDatabaseAccessRights(database.first))
                database_column_mut->insert(database.first);
        }

        block_to_filter.insert(ColumnWithTypeAndName(std::move(database_column_mut), std::make_shared<DataTypeString>(), "database"));

        /// Filter block with `database` column.
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

        if (!block_to_filter.rows())
            return;

        ColumnPtr database_column = block_to_filter.getByName("database").column;
        size_t rows = database_column->size();

        /// Add `table` column.
        MutableColumnPtr table_column_mut = ColumnString::create();
        IColumn::Offsets offsets(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            const std::string database_name = (*database_column)[i].get<std::string>();
            const DatabasePtr database = databases.at(database_name);
            offsets[i] = i ? offsets[i - 1] : 0;

            for (auto iterator = database->getIterator(context); iterator->isValid(); iterator->next())
            {
                const String & table_name = iterator->name();
                storages.emplace(std::piecewise_construct,
                    std::forward_as_tuple(database_name, table_name),
                    std::forward_as_tuple(iterator->table()));
                table_column_mut->insert(table_name);
                offsets[i] += 1;
            }
        }

        for (size_t i = 0; i < block_to_filter.columns(); ++i)
        {
            ColumnPtr & column = block_to_filter.safeGetByPosition(i).column;
            column = column->replicate(offsets);
        }

        block_to_filter.insert(ColumnWithTypeAndName(std::move(table_column_mut), std::make_shared<DataTypeString>(), "table"));
    }

    /// Filter block with `database` and `table` columns.
    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block_to_filter, context);

    if (!block_to_filter.rows())
        return;

    ColumnPtr filtered_database_column = block_to_filter.getByName("database").column;
    ColumnPtr filtered_table_column = block_to_filter.getByName("table").column;

    /// We compose the result.
    size_t rows = filtered_database_column->size();
    for (size_t row_no = 0; row_no < rows; ++row_no)
    {
        const std::string database_name = (*filtered_database_column)[row_no].get<std::string>();
        const std::string table_name = (*filtered_table_column)[row_no].get<std::string>();

        NamesAndTypesList columns;
        ColumnDefaults column_defaults;
        MergeTreeData::ColumnSizeByName column_sizes;

        {
            StoragePtr storage = storages.at(std::make_pair(database_name, table_name));
            TableStructureReadLockPtr table_lock;

            try
            {
                table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);
            }
            catch (const Exception & e)
            {
                /** There are case when IStorage::drop was called,
                  *  but we still own the object.
                  * Then table will throw exception at attempt to lock it.
                  * Just skip the table.
                  */
                if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
                    continue;
                else
                    throw;
            }

            columns = storage->getColumns().getAll();
            column_defaults = storage->getColumns().defaults;

            /** Info about sizes of columns for tables of MergeTree family.
              * NOTE: It is possible to add getter for this info to IStorage interface.
              */
            if (auto storage_concrete_plain = dynamic_cast<StorageMergeTree *>(storage.get()))
            {
                column_sizes = storage_concrete_plain->getData().getColumnSizes();
            }
            else if (auto storage_concrete_replicated = dynamic_cast<StorageReplicatedMergeTree *>(storage.get()))
            {
                column_sizes = storage_concrete_replicated->getData().getColumnSizes();
            }
        }

        for (const auto & column : columns)
        {
            size_t i = 0;

            res_columns[i++]->insert(database_name);
            res_columns[i++]->insert(table_name);
            res_columns[i++]->insert(column.name);
            res_columns[i++]->insert(column.type->getName());

            {
                const auto it = column_defaults.find(column.name);
                if (it == std::end(column_defaults))
                {
                    res_columns[i++]->insertDefault();
                    res_columns[i++]->insertDefault();
                }
                else
                {
                    res_columns[i++]->insert(toString(it->second.kind));
                    res_columns[i++]->insert(queryToString(it->second.expression));
                }
            }

            {
                const auto it = column_sizes.find(column.name);
                if (it == std::end(column_sizes))
                {
                    res_columns[i++]->insertDefault();
                    res_columns[i++]->insertDefault();
                    res_columns[i++]->insertDefault();
                }
                else
                {
                    res_columns[i++]->insert(static_cast<UInt64>(it->second.data_compressed));
                    res_columns[i++]->insert(static_cast<UInt64>(it->second.data_uncompressed));
                    res_columns[i++]->insert(static_cast<UInt64>(it->second.marks));
                }
            }
        }
    }
}

}
