#pragma once

#include <Storages/IStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Core/Defines.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{

/// A Storage that allows reading from a single MergeTree data part.
class StorageFroIMergeTreeDataPart : public ext::shared_ptr_helper<StorageFroIMergeTreeDataPart>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageFroIMergeTreeDataPart>;
public:
    String getName() const override { return "FroIMergeTreeDataPart"; }
    String getTableName() const override { return part->storage.getTableName() + " (part " + part->name + ")"; }
    String getDatabaseName() const override { return part->storage.getDatabaseName(); }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        unsigned num_streams) override
    {
        return MergeTreeDataSelectExecutor(part->storage).readFromParts(
            {part}, column_names, query_info, context, max_block_size, num_streams);
    }

    bool supportsIndexForIn() const override { return true; }

    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context) const override
    {
        return part->storage.mayBenefitFromIndexForIn(left_in_operand, query_context);
    }

protected:
    StorageFroIMergeTreeDataPart(const MergeTreeData::DataPartPtr & part_)
        : IStorage(part_->storage.getVirtuals()), part(part_)
    {
        setColumns(part_->storage.getColumns());
        setIndices(part_->storage.getIndices());
    }

private:
    MergeTreeData::DataPartPtr part;
};

}
