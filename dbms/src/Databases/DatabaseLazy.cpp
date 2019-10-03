#include <iomanip>

#include <Core/Settings.h>
#include <Databases/DatabaseLazy.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabasesCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Event.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNSUPPORTED_METHOD;
    extern const int CANNOT_CREATE_TABLE_FROM_METADATA;
    extern const int INCORRECT_FILE_NAME;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int SYNTAX_ERROR;
}


static constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

static size_t getLastModifiedEpochTime(const String & table_metadata_path) {
    Poco::File meta_file(table_metadata_path);

    if (meta_file.exists())
    {
        return meta_file.getLastModified().epochTime();
    }
    else
    {
        return static_cast<time_t>(0);
    }
}

DatabaseLazy::DatabaseLazy(const String & name_, const String & metadata_path_, time_t expiration_time_, const Context & context)
    : name(name_)
    , metadata_path(metadata_path_)
    , data_path(context.getPath() + "data/" + escapeForFileName(name) + "/")
    , expiration_time(expiration_time_)
    , log(&Logger::get("DatabaseLazy (" + name + ")"))
{
    Poco::File(getDataPath()).createDirectories();
}


void DatabaseLazy::loadTables(
    Context & /* context */,
    bool /* has_force_restore_data_flag */)
{
    DatabaseOnDisk::iterateTableFiles(*this, log, [this](const String & file_name) {
        const std::string table_name = file_name.substr(0, file_name.size() - 4);
        attachTable(table_name, nullptr);
    });
}


void DatabaseLazy::createTable(
    const Context & context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    clearExpiredTables();
    if (!endsWith(table->getName(), "Log"))
        throw Exception("Lazy engine can be used only with *Log tables.", ErrorCodes::UNSUPPORTED_METHOD);
    DatabaseOnDisk::createTable(*this, context, table_name, table, query);
}


void DatabaseLazy::removeTable(
    const Context & context,
    const String & table_name)
{
    clearExpiredTables();
    DatabaseOnDisk::removeTable(*this, context, table_name, log);
}

void DatabaseLazy::renameTable(
    const Context & context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    TableStructureWriteLockHolder & lock)
{
    clearExpiredTables();
    DatabaseOnDisk::renameTable<DatabaseLazy>(*this, context, table_name, to_database, to_table_name, lock);
}


time_t DatabaseLazy::getTableMetadataModificationTime(
    const Context & /* context */,
    const String & table_name)
{
    std::lock_guard lock(tables_mutex);
    auto it = tables_cache.find(table_name);
    if (it != tables_cache.end())
        return it->second.metadata_modification_time;
    else
        throw Exception("Table " + getDatabaseName() + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
}

ASTPtr DatabaseLazy::getCreateTableQuery(const Context & context, const String & table_name) const
{
    return DatabaseOnDisk::getCreateTableQuery(*this, context, table_name);
}

ASTPtr DatabaseLazy::tryGetCreateTableQuery(const Context & context, const String & table_name) const
{
    return DatabaseOnDisk::tryGetCreateTableQuery(*this, context, table_name);
}

ASTPtr DatabaseLazy::getCreateDatabaseQuery(const Context & context) const
{
    return DatabaseOnDisk::getCreateDatabaseQuery(*this, context);
}

void DatabaseLazy::alterTable(
    const Context & /* context */,
    const String & /* table_name */,
    const ColumnsDescription & /* columns */,
    const IndicesDescription & /* indices */,
    const ConstraintsDescription & /* constraints */,
    const ASTModifier & /* storage_modifier */)
{
    clearExpiredTables();
    throw Exception("ALTER query is not supported for Lazy database.", ErrorCodes::UNSUPPORTED_METHOD);
}


void DatabaseLazy::drop()
{
    DatabaseOnDisk::drop(*this);
}

bool DatabaseLazy::isTableExist(
    const Context & /* context */,
    const String & table_name) const
{
    clearExpiredTables();
    std::lock_guard lock(tables_mutex);
    return tables_cache.find(table_name) != tables_cache.end();
}

StoragePtr DatabaseLazy::tryGetTable(
    const Context & context,
    const String & table_name) const
{
    clearExpiredTables();
    {
        std::lock_guard lock(tables_mutex);
        auto it = tables_cache.find(table_name);
        if (it == tables_cache.end())
            throw Exception("Table " + getDatabaseName() + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

        if (it->second.table)
        {
            cache_expiration_queue.erase({it->second.last_touched, table_name});
            it->second.last_touched = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            cache_expiration_queue.emplace(it->second.last_touched, table_name);

            return it->second.table;
        }
    }
    
    return loadTable(context, table_name);
}

DatabaseIteratorPtr DatabaseLazy::getIterator(const Context & context, const FilterByNameFunction & filter_by_table_name)
{
    std::lock_guard lock(tables_mutex);
    Strings filtered_tables;
    for (const auto & [table_name, cached_table] : tables_cache)
    {
        if (!filter_by_table_name || filter_by_table_name(table_name))
            filtered_tables.push_back(table_name);
    }
    std::sort(filtered_tables.begin(), filtered_tables.end());
    return std::make_unique<DatabaseLazyIterator>(*this, context, std::move(filtered_tables));
}

bool DatabaseLazy::empty(const Context & /* context */) const
{
    return tables_cache.empty();
}

void DatabaseLazy::attachTable(const String & table_name, const StoragePtr & table)
{
    LOG_DEBUG(log, "attach table" << table_name);
    std::lock_guard lock(tables_mutex);
    time_t current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    if (!tables_cache.emplace(std::piecewise_construct,
                              std::forward_as_tuple(table_name),
                              std::forward_as_tuple(table,
                                                    current_time,
                                                    getLastModifiedEpochTime(getTableMetadataPath(table_name)))).second)
        throw Exception("Table " + getDatabaseName() + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
    if (!cache_expiration_queue.emplace(current_time, table_name).second)
        throw Exception("Failed to insert element to expiration queue.", ErrorCodes::LOGICAL_ERROR);
}

StoragePtr DatabaseLazy::detachTable(const String & table_name)
{
    StoragePtr res;
    {
        LOG_DEBUG(log, "detach table" << table_name);
        std::lock_guard lock(tables_mutex);
        auto it = tables_cache.find(table_name);
        if (it == tables_cache.end())
            throw Exception("Table " + getDatabaseName() + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        res = it->second.table;
        cache_expiration_queue.erase({it->second.last_touched, table_name});
        tables_cache.erase(it);
    }
    return res;
}

void DatabaseLazy::shutdown()
{
    TablesCache tables_snapshot;
    {
        std::lock_guard lock(tables_mutex);
        tables_snapshot = tables_cache;
    }

    for (const auto & kv : tables_snapshot)
    {
        if (kv.second.table)
            kv.second.table->shutdown();
    }

    std::lock_guard lock(tables_mutex);
    tables_cache.clear();
}

DatabaseLazy::~DatabaseLazy()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

String DatabaseLazy::getDataPath() const
{
    return data_path;
}

String DatabaseLazy::getMetadataPath() const
{
    return metadata_path;
}

String DatabaseLazy::getDatabaseName() const
{
    return name;
}

String DatabaseLazy::getTableMetadataPath(const String & table_name) const
{
    return DatabaseOnDisk::getTableMetadataPath(*this, table_name);
}

StoragePtr DatabaseLazy::loadTable(const Context & context, const String & table_name) const
{
    clearExpiredTables();

    LOG_DEBUG(log, "Load table '" << table_name << "' to cache.");

    const String table_metadata_path = getMetadataPath() + "/" + table_name + ".sql";

    String s;
    {
        char in_buf[METADATA_FILE_BUFFER_SIZE];
        ReadBufferFromFile in(table_metadata_path, METADATA_FILE_BUFFER_SIZE, -1, in_buf);
        readStringUntilEOF(s, in);
    }

    /** Empty files with metadata are generated after a rough restart of the server.
      * Remove these files to slightly reduce the work of the admins on startup.
      */
    if (s.empty())
    {
        // TODO: exception
        LOG_ERROR(log, "loadTable: File " << table_metadata_path << " is empty. Removing.");
        Poco::File(table_metadata_path).remove();
        return nullptr;
    }

    try
    {
        String table_name_;
        StoragePtr table;
        Context context_copy(context); /// some tables can change context, but not LogTables
        std::tie(table_name_, table) = createTableFromDefinition(
            s, name, getDataPath(), context_copy, false, "in file " + table_metadata_path);
        if (!endsWith(table->getName(), "Log"))
            throw Exception("Only *Log tables can be used with Lazy database engine.", ErrorCodes::LOGICAL_ERROR);
        {
            std::lock_guard lock(tables_mutex);
            auto it = tables_cache.find(table_name);
            if (it == tables_cache.end())
                throw Exception("Table " + getDatabaseName() + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

            cache_expiration_queue.erase({it->second.last_touched, table_name});
            it->second.last_touched = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            cache_expiration_queue.emplace(it->second.last_touched, table_name);

            return it->second.table = table;
        }
    }
    catch (const Exception & e)
    {
        throw Exception("Cannot create table from metadata file " + table_metadata_path + ", error: " + e.displayText() +
            ", stack trace:\n" + e.getStackTrace().toString(),
            ErrorCodes::CANNOT_CREATE_TABLE_FROM_METADATA);
    }
}

void DatabaseLazy::clearExpiredTables() const
{
    std::lock_guard lock(tables_mutex);
    while (!cache_expiration_queue.empty() &&
           (std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - cache_expiration_queue.begin()->last_touched) >= expiration_time)
    {
        auto it = tables_cache.find(cache_expiration_queue.begin()->table_name);
        if (!it->second.table.unique())
            continue;

        LOG_DEBUG(log, "Drop table '" << it->first << "' from cache.");
        /// Table can be already removed by detachTable.
        if (it != tables_cache.end())
            it->second.table.reset();
        cache_expiration_queue.erase(cache_expiration_queue.begin());
    }
}


DatabaseLazyIterator::DatabaseLazyIterator(DatabaseLazy & database_, const Context & context_, Strings && table_names_)
    : database(database_)
    , table_names(std::move(table_names_))
    , context(context_)
    , iterator(table_names.begin())
    , current_storage(nullptr)
{
}

void DatabaseLazyIterator::next()
{
    current_storage.reset();
    ++iterator;
    while (isValid() && !database.isTableExist(context, *iterator))
        ++iterator;
}

bool DatabaseLazyIterator::isValid() const
{
    return iterator != table_names.end();
}

const String & DatabaseLazyIterator::name() const
{
    return *iterator;
}

const StoragePtr & DatabaseLazyIterator::table() const
{
    if (!current_storage)
        current_storage = database.tryGetTable(context, *iterator);
    return current_storage;
}

}
