#include <DataStreams/AddingConstColumnBlockInputStream.h>
#include <DataStreams/narrowBlockInputStreams.h>
#include <DataStreams/LazyBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/AlterCommands.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <Databases/IDatabase.h>
#include <ext/range.h>
#include <algorithm>
#include <Parsers/queryToString.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Transforms/AddingConstColumnTransform.h>
#include <Processors/Transforms/ConvertingTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_PREWHERE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int BLOCKS_HAVE_DIFFERENT_STRUCTURE;
    extern const int SAMPLING_NOT_SUPPORTED;
}


StorageMerge::StorageMerge(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const String & source_database_,
    const String & table_name_regexp_,
    const Context & context_)
    : IStorage(table_id_, ColumnsDescription({{"_table", std::make_shared<DataTypeString>()}}, true))
    , source_database(source_database_)
    , table_name_regexp(table_name_regexp_)
    , global_context(context_)
{
    setColumns(columns_);
}


/// NOTE: structure of underlying tables as well as their set are not constant,
///       so the results of these methods may become obsolete after the call.

NameAndTypePair StorageMerge::getColumn(const String & column_name) const
{
    if (!IStorage::hasColumn(column_name))
    {
        auto first_table = getFirstTable([](auto &&) { return true; });
        if (first_table)
            return first_table->getColumn(column_name);
    }

    return IStorage::getColumn(column_name);
}


bool StorageMerge::hasColumn(const String & column_name) const
{
    if (!IStorage::hasColumn(column_name))
    {
        auto first_table = getFirstTable([](auto &&) { return true; });
        if (first_table)
            return first_table->hasColumn(column_name);
    }

    return true;
}


template <typename F>
StoragePtr StorageMerge::getFirstTable(F && predicate) const
{
    auto iterator = getDatabaseIterator(global_context);

    while (iterator->isValid())
    {
        auto & table = iterator->table();
        if (table.get() != this && predicate(table))
            return table;

        iterator->next();
    }

    return {};
}


bool StorageMerge::isRemote() const
{
    auto first_remote_table = getFirstTable([](const StoragePtr & table) { return table->isRemote(); });
    return first_remote_table != nullptr;
}


bool StorageMerge::mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context & query_context) const
{
    /// It's beneficial if it is true for at least one table.
    StorageListWithLocks selected_tables = getSelectedTables(query_context.getCurrentQueryId());

    size_t i = 0;
    for (const auto & table : selected_tables)
    {
        if (std::get<0>(table)->mayBenefitFromIndexForIn(left_in_operand, query_context))
            return true;

        ++i;
        /// For simplicity reasons, check only first ten tables.
        if (i > 10)
            break;
    }

    return false;
}


QueryProcessingStage::Enum StorageMerge::getQueryProcessingStage(const Context & context) const
{
    auto stage_in_source_tables = QueryProcessingStage::FetchColumns;

    DatabaseTablesIteratorPtr iterator = getDatabaseIterator(context);

    size_t selected_table_size = 0;

    while (iterator->isValid())
    {
        auto & table = iterator->table();
        if (table.get() != this)
        {
            ++selected_table_size;
            stage_in_source_tables = std::max(stage_in_source_tables, table->getQueryProcessingStage(context));
        }

        iterator->next();
    }

    return selected_table_size == 1 ? stage_in_source_tables : std::min(stage_in_source_tables, QueryProcessingStage::WithMergeableState);
}


Pipes StorageMerge::readWithProcessors(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    unsigned num_streams)
{
    Pipes res;

    bool has_table_virtual_column = false;
    Names real_column_names;
    real_column_names.reserve(column_names.size());

    for (const auto & column_name : column_names)
    {
        if (column_name == "_table" && isVirtualColumn(column_name))
            has_table_virtual_column = true;
        else
            real_column_names.push_back(column_name);
    }

    /** Just in case, turn off optimization "transfer to PREWHERE",
      * since there is no certainty that it works when one of table is MergeTree and other is not.
      */
    Context modified_context = context;
    modified_context.getSettingsRef().optimize_move_to_prewhere = false;

    /// What will be result structure depending on query processed stage in source tables?
    Block header = getQueryHeader(column_names, query_info, context, processed_stage);

    /** First we make list of selected tables to find out its size.
      * This is necessary to correctly pass the recommended number of threads to each table.
      */
    StorageListWithLocks selected_tables = getSelectedTables(
        query_info.query, has_table_virtual_column, context.getCurrentQueryId());

    if (selected_tables.empty())
        /// FIXME: do we support sampling in this case?
        return createSources(
            query_info, processed_stage, max_block_size, header, {}, real_column_names, modified_context, 0, has_table_virtual_column);

    size_t tables_count = selected_tables.size();
    Float64 num_streams_multiplier = std::min(unsigned(tables_count), std::max(1U, unsigned(context.getSettingsRef().max_streams_multiplier_for_merge_tables)));
    num_streams *= num_streams_multiplier;
    size_t remaining_streams = num_streams;

    InputSortingInfoPtr input_sorting_info;
    if (query_info.order_by_optimizer)
    {
        for (auto it = selected_tables.begin(); it != selected_tables.end(); ++it)
        {
            auto current_info = query_info.order_by_optimizer->getInputOrder(std::get<0>(*it));
            if (it == selected_tables.begin())
                input_sorting_info = current_info;
            else if (!current_info || (input_sorting_info && *current_info != *input_sorting_info))
                input_sorting_info.reset();

            if (!input_sorting_info)
                break;
        }

        query_info.input_sorting_info = input_sorting_info;
    }

    for (const auto & table : selected_tables)
    {
        size_t current_need_streams = tables_count >= num_streams ? 1 : (num_streams / tables_count);
        size_t current_streams = std::min(current_need_streams, remaining_streams);
        remaining_streams -= current_streams;
        current_streams = std::max(size_t(1), current_streams);

        auto & storage = std::get<0>(table);

        /// If sampling requested, then check that table supports it.
        if (query_info.query->as<ASTSelectQuery>()->sample_size() && !storage->supportsSampling())
            throw Exception("Illegal SAMPLE: table doesn't support sampling", ErrorCodes::SAMPLING_NOT_SUPPORTED);

        auto source_pipes = createSources(
                query_info, processed_stage, max_block_size, header, *it, real_column_names, modified_context,
                current_streams, has_table_virtual_column);

        for (auto & pipe : source_pipes)
            res.emplace_back(std::move(pipe));
    }

    if (res.empty())
        return res;

    return narrowPipes(std::move(res), num_streams);
}

Pipes StorageMerge::createSources(const SelectQueryInfo & query_info, const QueryProcessingStage::Enum & processed_stage,
    const UInt64 max_block_size, const Block & header, const StorageWithLockAndName & storage_with_lock,
    Names & real_column_names,
    Context & modified_context, size_t streams_num, bool has_table_virtual_column,
    bool concat_streams)
{
    auto & [storage, struct_lock, table_name] = storage_with_lock;
    SelectQueryInfo modified_query_info = query_info;
    modified_query_info.query = query_info.query->clone();

    VirtualColumnUtils::rewriteEntityInAst(modified_query_info.query, "_table", table_name);

    Pipes pipes;

    if (!storage)
    {
        pipes.emplace_back(
            InterpreterSelectQuery(modified_query_info.query, modified_context,
                                   std::make_shared<OneBlockInputStream>(header),
                                   SelectQueryOptions(processed_stage).analyze()).executeWithProcessors().getPipe());

        return pipes;
    }

    if (processed_stage <= storage->getQueryProcessingStage(modified_context))
    {
        /// If there are only virtual columns in query, you must request at least one other column.
        if (real_column_names.empty())
            real_column_names.push_back(ExpressionActions::getSmallestColumn(storage->getColumns().getAllPhysical()));

        if (storage->supportProcessorsPipeline())
            pipes = storage->readWithProcessors(real_column_names, modified_query_info, modified_context, processed_stage, max_block_size, UInt32(streams_num));
        else
        {
            auto streams = storage->read(real_column_names, modified_query_info, modified_context, processed_stage, max_block_size, UInt32(streams_num));
            for (auto & stream : streams)
                pipes.emplace_back(std::make_shared<SourceFromInputStream>(std::move(stream)));
        }
    }
    else if (processed_stage > storage->getQueryProcessingStage(modified_context))
    {
        modified_query_info.query->as<ASTSelectQuery>()->replaceDatabaseAndTable(source_database, table_name);

        /// Maximum permissible parallelism is streams_num
        modified_context.getSettingsRef().max_threads = UInt64(streams_num);
        modified_context.getSettingsRef().max_streams_to_max_threads_ratio = 1;

        InterpreterSelectQuery interpreter{modified_query_info.query, modified_context, SelectQueryOptions(processed_stage)};
        Pipe pipe = interpreter.executeWithProcessors().getPipe();

        /** Materialization is needed, since from distributed storage the constants come materialized.
          * If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
          * And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
          */
        pipe.addSimpleTransform(std::make_shared<MaterializingTransform>(pipe.getHeader()));
    }

    if (!pipes.empty())
    {
        if (concat_streams && pipes.size() > 1)
        {
            auto concat = std::make_shared<ConcatProcessor>(pipes.at(0).getHeader(), pipes.size());
            Pipe pipe(std::move(pipes), std::move(concat));

            pipes.clear();
            pipes.emplace_back(std::move(pipe));
        }

        for (auto & pipe : pipes)
        {
            if (has_table_virtual_column)
                pipe.addSimpleTransform(std::make_shared<AddingConstColumnTransform<String>>(
                    pipe.getHeader(), std::make_shared<DataTypeString>(), table_name, "_table"));

            /// Subordinary tables could have different but convertible types, like numeric types of different width.
            /// We must return streams with structure equals to structure of Merge table.
            convertingSourceStream(header, modified_context, modified_query_info.query, pipe, processed_stage);

            pipe.addTableLock(struct_lock);
        }
    }

    return pipes;
}


StorageMerge::StorageListWithLocks StorageMerge::getSelectedTables(const String & query_id) const
{
    StorageListWithLocks selected_tables;
    auto iterator = getDatabaseIterator(global_context);

    while (iterator->isValid())
    {
        auto & table = iterator->table();
        if (table.get() != this)
            selected_tables.emplace_back(table, table->lockStructureForShare(false, query_id), iterator->name());

        iterator->next();
    }

    return selected_tables;
}


StorageMerge::StorageListWithLocks StorageMerge::getSelectedTables(const ASTPtr & query, bool has_virtual_column, const String & query_id) const
{
    StorageListWithLocks selected_tables;
    DatabaseTablesIteratorPtr iterator = getDatabaseIterator(global_context);

    auto virtual_column = ColumnString::create();

    while (iterator->isValid())
    {
        StoragePtr storage = iterator->table();

        if (query && query->as<ASTSelectQuery>()->prewhere() && !storage->supportsPrewhere())
            throw Exception("Storage " + storage->getName() + " doesn't support PREWHERE.", ErrorCodes::ILLEGAL_PREWHERE);

        if (storage.get() != this)
        {
            selected_tables.emplace_back(storage, storage->lockStructureForShare(false, query_id), iterator->name());
            virtual_column->insert(iterator->name());
        }

        iterator->next();
    }

    if (has_virtual_column)
    {
        Block virtual_columns_block = Block{ColumnWithTypeAndName(std::move(virtual_column), std::make_shared<DataTypeString>(), "_table")};
        VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, global_context);
        auto values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_table");

        /// Remove unused tables from the list
        selected_tables.remove_if([&] (const auto & elem) { return values.find(std::get<2>(elem)) == values.end(); });
    }

    return selected_tables;
}


DatabaseTablesIteratorPtr StorageMerge::getDatabaseIterator(const Context & context) const
{
    checkStackSize();
    auto database = context.getDatabase(source_database);
    auto table_name_match = [this](const String & table_name_) { return table_name_regexp.match(table_name_); };
    return database->getTablesIterator(global_context, table_name_match);
}


void StorageMerge::checkAlterIsPossible(const AlterCommands & commands, const Settings & /* settings */)
{
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN && command.type != AlterCommand::Type::COMMENT_COLUMN)
            throw Exception(
                "Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                ErrorCodes::NOT_IMPLEMENTED);
    }
}

void StorageMerge::alter(
    const AlterCommands & params, const Context & context, TableStructureWriteLockHolder & table_lock_holder)
{
    lockStructureExclusively(table_lock_holder, context.getCurrentQueryId());
    auto table_id = getStorageID();

    StorageInMemoryMetadata storage_metadata = getInMemoryMetadata();
    params.apply(storage_metadata);
    context.getDatabase(table_id.database_name)->alterTable(context, table_id.table_name, storage_metadata);
    setColumns(storage_metadata.columns);
}

Block StorageMerge::getQueryHeader(
    const Names & column_names, const SelectQueryInfo & query_info, const Context & context, QueryProcessingStage::Enum processed_stage)
{
    switch (processed_stage)
    {
        case QueryProcessingStage::FetchColumns:
        {
            Block header = getSampleBlockForColumns(column_names);
            if (query_info.prewhere_info)
            {
                query_info.prewhere_info->prewhere_actions->execute(header);
                header = materializeBlock(header);
                if (query_info.prewhere_info->remove_prewhere_column)
                    header.erase(query_info.prewhere_info->prewhere_column_name);
            }
            return header;
        }
        case QueryProcessingStage::WithMergeableState:
        case QueryProcessingStage::Complete:
            return materializeBlock(InterpreterSelectQuery(
                query_info.query, context, std::make_shared<OneBlockInputStream>(getSampleBlockForColumns(column_names)),
                SelectQueryOptions(processed_stage).analyze()).getSampleBlock());
    }
    throw Exception("Logical Error: unknown processed stage.", ErrorCodes::LOGICAL_ERROR);
}

void StorageMerge::convertingSourceStream(const Block & header, const Context & context, ASTPtr & query,
                                          Pipe & pipe, QueryProcessingStage::Enum processed_stage)
{
    Block before_block_header = pipe.getHeader();
    pipe.addSimpleTransform(std::make_shared<ConvertingTransform>(before_block_header, header, ConvertingTransform::MatchColumnsMode::Name, context));

    auto where_expression = query->as<ASTSelectQuery>()->where();

    if (!where_expression)
        return;

    for (size_t column_index : ext::range(0, header.columns()))
    {
        ColumnWithTypeAndName header_column = header.getByPosition(column_index);
        ColumnWithTypeAndName before_column = before_block_header.getByName(header_column.name);
        /// If the processed_stage greater than FetchColumns and the block structure between streams is different.
        /// the where expression maybe invalid because of convertingBlockInputStream.
        /// So we need to throw exception.
        if (!header_column.type->equals(*before_column.type.get()) && processed_stage > QueryProcessingStage::FetchColumns)
        {
            NamesAndTypesList source_columns = getSampleBlock().getNamesAndTypesList();
            NameAndTypePair virtual_column = getColumn("_table");
            source_columns.insert(source_columns.end(), virtual_column);
            auto syntax_result = SyntaxAnalyzer(context).analyze(where_expression, source_columns);
            ExpressionActionsPtr actions = ExpressionAnalyzer{where_expression, syntax_result, context}.getActions(false, false);
            Names required_columns = actions->getRequiredColumns();

            for (const auto & required_column : required_columns)
            {
                if (required_column == header_column.name)
                    throw Exception("Block structure mismatch in Merge Storage: different types:\n" + before_block_header.dumpStructure()
                                    + "\n" + header.dumpStructure(), ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);
            }
        }

    }
}


void registerStorageMerge(StorageFactory & factory)
{
    factory.registerStorage("Merge", [](const StorageFactory::Arguments & args)
    {
        /** In query, the name of database is specified as table engine argument which contains source tables,
          *  as well as regex for source-table names.
          */

        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception("Storage Merge requires exactly 2 parameters"
                " - name of source database and regexp for table names.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);
        engine_args[1] = evaluateConstantExpressionAsLiteral(engine_args[1], args.local_context);

        String source_database = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        String table_name_regexp = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        return StorageMerge::create(
            args.table_id, args.columns,
            source_database, table_name_regexp, args.context);
    });
}

}
