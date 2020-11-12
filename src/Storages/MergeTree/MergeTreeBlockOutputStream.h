#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

class Block;
class StorageMergeTree;


class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
    MergeTreeBlockOutputStream(StorageMergeTree & storage_, const StorageMetadataPtr metadata_snapshot_, size_t max_parts_per_block_, const Context & context_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , max_parts_per_block(max_parts_per_block_)
        , context(context_)
    {
    }

    Block getHeader() const override;
    void write(const Block & block) override;
    Block mergeBlock(const Block & block);

private:
    StorageMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t max_parts_per_block;
    const Context & context;
};

}
