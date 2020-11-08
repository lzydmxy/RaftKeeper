
#include <Storages/RocksDB/EmbeddedRocksDBBlockOutputStream.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

Block EmbeddedRocksDBBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}

void EmbeddedRocksDBBlockOutputStream::write(const Block & block)
{
    metadata_snapshot->check(block, true);
    auto rows = block.rows();

    WriteBufferFromOwnString wb_key;
    WriteBufferFromOwnString wb_value;

    rocksdb::WriteBatch batch;
    auto columns = metadata_snapshot->getColumns();

    for (size_t i = 0; i < rows; i++)
    {
        wb_key.restart();
        wb_value.restart();

        for (const auto & col : columns)
        {
            const auto & type = block.getByName(col.name).type;
            const auto & column = block.getByName(col.name).column;
            if (col.name == storage.primary_key)
                type->serializeBinary(*column, i, wb_key);
            else
                type->serializeBinary(*column, i, wb_value);
        }
        batch.Put(wb_key.str(), wb_value.str());
    }
    auto status = storage.rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
}

}
