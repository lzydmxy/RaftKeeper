#pragma once

#include <Core/Names.h>
#include <Common/Exception.h>
#include <Common/RWLock.h>
#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <Databases/IDatabase.h>
#include <Storages/AlterCommands.h>
#include <Storages/ITableDeclaration.h>
#include <Storages/SelectQueryInfo.h>
#include <shared_mutex>
#include <memory>
#include <optional>
#include <Common/ActionLock.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_DROPPED;
    extern const int NOT_IMPLEMENTED;
}

class Context;
class IBlockInputStream;
class IBlockOutputStream;

using StorageActionBlockType = size_t;

using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockInputStreams = std::vector<BlockInputStreamPtr>;

class ASTCreateQuery;

class IStorage;

using StoragePtr = std::shared_ptr<IStorage>;
using StorageWeakPtr = std::weak_ptr<IStorage>;

struct Settings;

class AlterCommands;
class MutationCommands;
class PartitionCommands;


/** Does not allow changing the table description (including rename and delete the table).
  * If during any operation the table structure should remain unchanged, you need to hold such a lock for all of its time.
  * For example, you need to hold such a lock for the duration of the entire SELECT or INSERT query and for the whole time the merge of the set of parts
  *  (but between the selection of parts for the merge and their merging, the table structure can change).
  * NOTE: This is a lock to "read" the table's description. To change the table description, you need to take the TableStructureWriteLock.
  */
class TableStructureReadLock
{
private:
    friend class IStorage;

    StoragePtr storage;
    /// Order is important.
    RWLockImpl::LockHandler data_lock;
    RWLockImpl::LockHandler structure_lock;

public:
    TableStructureReadLock(StoragePtr storage_, bool lock_structure, bool lock_data, const std::string & who);
};


using TableStructureReadLockPtr = std::shared_ptr<TableStructureReadLock>;
using TableStructureReadLocks = std::vector<TableStructureReadLockPtr>;

using TableStructureWriteLock = RWLockImpl::LockHandler;
using TableDataWriteLock = RWLockImpl::LockHandler;
using TableFullWriteLock = std::pair<TableDataWriteLock, TableStructureWriteLock>;


/** Storage. Responsible for
  * - storage of the table data;
  * - the definition in which files (or not in files) the data is stored;
  * - data lookups and appends;
  * - data storage structure (compression, etc.)
  * - concurrent access to data (locks, etc.)
  */
class IStorage : public std::enable_shared_from_this<IStorage>, private boost::noncopyable, public ITableDeclaration
{
public:
    /// The main name of the table type (for example, StorageMergeTree).
    virtual std::string getName() const = 0;

    /** The name of the table.
      */
    virtual std::string getTableName() const = 0;

    /** Returns true if the storage receives data from a remote server or servers. */
    virtual bool isRemote() const { return false; }

    /** Returns true if the storage supports queries with the SAMPLE section. */
    virtual bool supportsSampling() const { return false; }

    /** Returns true if the storage supports queries with the FINAL section. */
    virtual bool supportsFinal() const { return false; }

    /** Returns true if the storage supports queries with the PREWHERE section. */
    virtual bool supportsPrewhere() const { return false; }

    /** Returns true if the storage replicates SELECT, INSERT and ALTER commands among replicas. */
    virtual bool supportsReplication() const { return false; }

    /** Returns true if the storage supports deduplication of inserted data blocks . */
    virtual bool supportsDeduplication() const { return false; }

    /** Does not allow you to change the structure or name of the table.
      * If you change the data in the table, you will need to specify will_modify_data = true.
      * This will take an extra lock that does not allow starting ALTER MODIFY.
      * Parameter 'who' identifies a client of the lock (ALTER query, merge process, etc), used for diagnostic purposes.
      *
      * WARNING: You need to call methods from ITableDeclaration under such a lock. Without it, they are not thread safe.
      * WARNING: To avoid deadlocks, this method must not be called under lock of Context.
      */
    TableStructureReadLockPtr lockStructure(bool will_modify_data, const std::string & who)
    {
        TableStructureReadLockPtr res = std::make_shared<TableStructureReadLock>(shared_from_this(), true, will_modify_data, who);
        if (is_dropped)
            throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
        return res;
    }

    /** Does not allow reading the table structure. It is taken for ALTER, RENAME and DROP, TRUNCATE.
      */
    TableFullWriteLock lockForAlter(const std::string & who = "Alter")
    {
        /// The calculation order is important.
        auto res_data_lock = lockDataForAlter(who);
        auto res_structure_lock = lockStructureForAlter(who);

        return {std::move(res_data_lock), std::move(res_structure_lock)};
    }

    /** Does not allow changing the data in the table. (Moreover, does not give a look at the structure of the table with the intention to change the data).
      * It is taken during write temporary data in ALTER MODIFY.
      * Under this lock, you can take lockStructureForAlter() to change the structure of the table.
      */
    TableDataWriteLock lockDataForAlter(const std::string & who = "Alter")
    {
        auto res = data_lock->getLock(RWLockImpl::Write, who);
        if (is_dropped)
            throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
        return res;
    }

    TableStructureWriteLock lockStructureForAlter(const std::string & who = "Alter")
    {
        auto res = structure_lock->getLock(RWLockImpl::Write, who);
        if (is_dropped)
            throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);
        return res;
    }

    /** Returns stage to which query is going to be processed in read() function.
      * (Normally, the function only reads the columns from the list, but in other cases,
      *  for example, the request can be partially processed on a remote server.)
      */
    virtual QueryProcessingStage::Enum getQueryProcessingStage(const Context &) const { return QueryProcessingStage::FetchColumns; }

    /** Read a set of columns from the table.
      * Accepts a list of columns to read, as well as a description of the query,
      *  from which information can be extracted about how to retrieve data
      *  (indexes, locks, etc.)
      * Returns a stream with which you can read data sequentially
      *  or multiple streams for parallel data reading.
      * The `processed_stage` must be the result of getQueryProcessingStage() function.
      *
      * context contains settings for one query.
      * Usually Storage does not care about these settings, since they are used in the interpreter.
      * But, for example, for distributed query processing, the settings are passed to the remote server.
      *
      * num_streams - a recommendation, how many streams to return,
      *  if the storage can return a different number of streams.
      *
      * It is guaranteed that the structure of the table will not change over the lifetime of the returned streams (that is, there will not be ALTER, RENAME and DROP).
      */
    virtual BlockInputStreams read(
        const Names & /*column_names*/,
        const SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/)
    {
        throw Exception("Method read is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Writes the data to a table.
      * Receives a description of the query, which can contain information about the data write method.
      * Returns an object by which you can write data sequentially.
      *
      * It is guaranteed that the table structure will not change over the lifetime of the returned streams (that is, there will not be ALTER, RENAME and DROP).
      */
    virtual BlockOutputStreamPtr write(
        const ASTPtr & /*query*/,
        const Settings & /*settings*/)
    {
        throw Exception("Method write is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Delete the table data. Called before deleting the directory with the data.
      * The method can be called only after detaching table from Context (when no queries are performed with table).
      * The table is not usable during and after call to this method.
      * If you do not need any action other than deleting the directory with data, you can leave this method blank.
      */
    virtual void drop() {}

    /** Clear the table data and leave it empty.
      * Must be called under lockForAlter.
      */
    virtual void truncate(const ASTPtr & /*query*/)
    {
        throw Exception("Truncate is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Rename the table.
      * Renaming a name in a file with metadata, the name in the list of tables in the RAM, is done separately.
      * In this function, you need to rename the directory with the data, if any.
      * Called when the table structure is locked for write.
      */
    virtual void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & /*new_table_name*/)
    {
        throw Exception("Method rename is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** ALTER tables in the form of column changes that do not affect the change to Storage or its parameters.
      * This method must fully execute the ALTER query, taking care of the locks itself.
      * To update the table metadata on disk, this method should call InterpreterAlterQuery::updateMetadata.
      */
    virtual void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
    {
        for (const auto & param : params)
        {
            if (param.is_mutable())
                throw Exception("Method alter supports only change comment of column for storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
        }

        auto lock = lockStructureForAlter(__PRETTY_FUNCTION__);
        auto new_columns = getColumns();
        params.apply(new_columns);
        context.getDatabase(database_name)->alterTable(context, table_name, new_columns, {});
        setColumns(std::move(new_columns));
    }

    /** ALTER tables with regard to its partitions.
      * Should handle locks for each command on its own.
      */
    virtual void alterPartition(const ASTPtr & /* query */, const PartitionCommands & /* commands */, const Context & /* context */)
    {
        throw Exception("Partition operations are not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Perform any background work. For example, combining parts in a MergeTree type table.
      * Returns whether any work has been done.
      */
    virtual bool optimize(const ASTPtr & /*query*/, const ASTPtr & /*partition*/, bool /*final*/, bool /*deduplicate*/, const Context & /*context*/)
    {
        throw Exception("Method optimize is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Mutate the table contents
    virtual void mutate(const MutationCommands &, const Context &)
    {
        throw Exception("Mutations are not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** If the table have to do some complicated work on startup,
      *  that must be postponed after creation of table object
      *  (like launching some background threads),
      *  do it in this method.
      * You should call this method after creation of object.
      * By default, does nothing.
      * Cannot be called simultaneously by multiple threads.
      */
    virtual void startup() {}

    /** If the table have to do some complicated work when destroying an object - do it in advance.
      * For example, if the table contains any threads for background work - ask them to complete and wait for completion.
      * By default, does nothing.
      * Can be called simultaneously from different threads, even after a call to drop().
      */
    virtual void shutdown() {}

    /// Asks table to stop executing some action identified by action_type
    /// If table does not support such type of lock, and empty lock is returned
    virtual ActionLock getActionLock(StorageActionBlockType /* action_type */)
    {
        return {};
    }

    bool is_dropped{false};

    /// Does table support index for IN sections
    virtual bool supportsIndexForIn() const { return false; }

    /// Provides a hint that the storage engine may evaluate the IN-condition by using an index.
    virtual bool mayBenefitFromIndexForIn(const ASTPtr & /* left_in_operand */) const { return false; }

    /// Checks validity of the data
    virtual bool checkData() const { throw Exception("Check query is not supported for " + getName() + " storage", ErrorCodes::NOT_IMPLEMENTED); }

    /// Checks that table could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    virtual void checkTableCanBeDropped() const {}

    /// Checks that Partition could be dropped right now
    /// Otherwise - throws an exception with detailed information.
    /// We do not use mutex because it is not very important that the size could change during the operation.
    virtual void checkPartitionCanBeDropped(const ASTPtr & /*partition*/) {}

    /** Notify engine about updated dependencies for this storage. */
    virtual void updateDependencies() {}

    /// Returns data path if storage supports it, empty string otherwise.
    virtual String getDataPath() const { return {}; }

    /// Returns sampling expression for storage or nullptr if there is no.
    virtual ASTPtr getSamplingExpression() const { return nullptr; }

    /// Returns primary expression for storage or nullptr if there is no.
    virtual ASTPtr getPrimaryExpression() const { return nullptr; }

    /// Returns partition expression for storage or nullptr if there is no.
    virtual ASTPtr getPartitionExpression() const { return nullptr; }

    /// Returns secondary expression for storage or nullptr if there is no.
    virtual ASTPtr getOrderExpression() const { return nullptr; }

    /// Returns sampling key names for storage or empty vector if there is no.
    virtual Names getSamplingExpressionNames() const { return {}; }

    /// Returns primary key names for storage or empty vector if there is no.
    virtual Names getPrimaryExpressionNames() const { return {}; }

    /// Returns partition key names for storage or empty vector if there is no.
    virtual Names getPartitionExpressionNames() const { return {}; }

    /// Returns order key names for storage or empty vector if there is no.
    virtual Names getOrderExpressionNames() const { return {}; }

    using ITableDeclaration::ITableDeclaration;
    using std::enable_shared_from_this<IStorage>::shared_from_this;

private:
    friend class TableStructureReadLock;

    /// You always need to take the next two locks in this order.

    /** It is taken for read for the entire INSERT query and the entire merge of the parts (for MergeTree).
      * It is taken for write for the entire time ALTER MODIFY.
      *
      * Formally:
      * Taking a write lock ensures that:
      *  1) the data in the table will not change while the lock is alive,
      *  2) all changes to the data after releasing the lock will be based on the structure of the table at the time after the lock was released.
      * You need to take for read for the entire time of the operation that changes the data.
      */
    mutable RWLock data_lock = RWLockImpl::create();

    /** Lock for multiple columns and path to table. It is taken for write at RENAME, ALTER (for ALTER MODIFY for a while) and DROP.
      * It is taken for read for the whole time of SELECT, INSERT and merge parts (for MergeTree).
      *
      * Taking this lock for writing is a strictly "stronger" operation than taking parts_writing_lock for write record.
      * That is, if this lock is taken for write, you should not worry about `parts_writing_lock`.
      * parts_writing_lock is only needed for cases when you do not want to take `table_structure_lock` for long operations (ALTER MODIFY).
      */
    mutable RWLock structure_lock = RWLockImpl::create();
};

/// table name -> table
using Tables = std::map<String, StoragePtr>;

}
