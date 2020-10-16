#pragma once

#include <Storages/IStorage.h>
#include <Processors/Pipe.h>


namespace DB
{

class StorageProxy : public IStorage
{
public:

    StorageProxy(const StorageID & table_id_) : IStorage(table_id_) {}

    virtual StoragePtr getNested() const = 0;

    String getName() const override { return "StorageProxy"; }

    bool isRemote() const override { return getNested()->isRemote(); }
    bool isView() const override { return getNested()->isView(); }
    bool supportsSampling() const override { return getNested()->supportsSampling(); }
    bool supportsFinal() const override { return getNested()->supportsFinal(); }
    bool supportsPrewhere() const override { return getNested()->supportsPrewhere(); }
    bool supportsReplication() const override { return getNested()->supportsReplication(); }
    bool supportsParallelInsert() const override { return getNested()->supportsParallelInsert(); }
    bool supportsDeduplication() const override { return getNested()->supportsDeduplication(); }
    bool supportsSettings() const override { return getNested()->supportsSettings(); }
    bool noPushingToViews() const override { return getNested()->noPushingToViews(); }
    bool hasEvenlyDistributedRead() const override { return getNested()->hasEvenlyDistributedRead(); }

    ColumnSizeByName getColumnSizes() const override { return getNested()->getColumnSizes(); }
    NamesAndTypesList getVirtuals() const override { return getNested()->getVirtuals(); }
    QueryProcessingStage::Enum getQueryProcessingStage(const Context & context, QueryProcessingStage::Enum to_stage, const ASTPtr & ast) const override
    {
        return getNested()->getQueryProcessingStage(context, to_stage, ast);
    }

    BlockInputStreams watch(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override
    {
        return getNested()->watch(column_names, query_info, context, processed_stage, max_block_size, num_streams);
    }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override
    {
        return getNested()->read(column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    }

    BlockOutputStreamPtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const Context & context) override
    {
        return getNested()->write(query, metadata_snapshot, context);
    }

    void drop() override { getNested()->drop(); }

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const Context & context,
        TableExclusiveLockHolder & lock) override
    {
        getNested()->truncate(query, metadata_snapshot, context, lock);
    }

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override
    {
        getNested()->rename(new_path_to_table_data, new_table_id);
        IStorage::renameInMemory(new_table_id);
    }

    void renameInMemory(const StorageID & new_table_id) override
    {
        getNested()->renameInMemory(new_table_id);
        IStorage::renameInMemory(new_table_id);
    }

    void alter(const AlterCommands & params, const Context & context, TableLockHolder & alter_lock_holder) override
    {
        getNested()->alter(params, context, alter_lock_holder);
        IStorage::setInMemoryMetadata(getNested()->getInMemoryMetadata());
    }

    void checkAlterIsPossible(const AlterCommands & commands, const Settings & settings) const override
    {
        getNested()->checkAlterIsPossible(commands, settings);
    }

    Pipe alterPartition(
            const ASTPtr & query,
            const StorageMetadataPtr & metadata_snapshot,
            const PartitionCommands & commands,
            const Context & context) override
    {
        return getNested()->alterPartition(query, metadata_snapshot, commands, context);
    }

    void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const override
    {
        getNested()->checkAlterPartitionIsPossible(commands, metadata_snapshot, settings);
    }

    bool optimize(
            const ASTPtr & query,
            const StorageMetadataPtr & metadata_snapshot,
            const ASTPtr & partition,
            bool final,
            bool deduplicate,
            const Context & context) override
    {
        return getNested()->optimize(query, metadata_snapshot, partition, final, deduplicate, context);
    }

    void mutate(const MutationCommands & commands, const Context & context) override { getNested()->mutate(commands, context); }

    CancellationCode killMutation(const String & mutation_id) override { return getNested()->killMutation(mutation_id); }

    void startup() override { getNested()->startup(); }
    void shutdown() override { getNested()->shutdown(); }

    ActionLock getActionLock(StorageActionBlockType action_type) override { return getNested()->getActionLock(action_type); }

    bool supportsIndexForIn() const override { return getNested()->supportsIndexForIn(); }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context, const StorageMetadataPtr & metadata_snapshot) const override
    {
        return getNested()->mayBenefitFromIndexForIn(left_in_operand, query_context, metadata_snapshot);
    }

    CheckResults checkData(const ASTPtr & query , const Context & context) override { return getNested()->checkData(query, context); }
    void checkTableCanBeDropped() const override { getNested()->checkTableCanBeDropped(); }
    void checkPartitionCanBeDropped(const ASTPtr & partition) override { getNested()->checkPartitionCanBeDropped(partition); }
    Strings getDataPaths() const override { return getNested()->getDataPaths(); }
    StoragePolicyPtr getStoragePolicy() const override { return getNested()->getStoragePolicy(); }
    std::optional<UInt64> totalRows() const override { return getNested()->totalRows(); }
    std::optional<UInt64> totalBytes() const override { return getNested()->totalBytes(); }
    std::optional<UInt64> lifetimeRows() const override { return getNested()->lifetimeRows(); }
    std::optional<UInt64> lifetimeBytes() const override { return getNested()->lifetimeBytes(); }

};


}

