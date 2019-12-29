#include <Interpreters/Set.h>
#include <DataStreams/materializeBlock.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <Storages/IStorage.h>
#include <iomanip>


namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}


CreatingSetsBlockInputStream::CreatingSetsBlockInputStream(
    const BlockInputStreamPtr & input,
    const SubqueriesForSets & subqueries_for_sets_,
    const Context & context_)
    : subqueries_for_sets(subqueries_for_sets_)
    , context(context_)
{
    const Settings & settings = context.getSettingsRef();
    network_transfer_limits = SizeLimits(
        settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode);

    for (auto & elem : subqueries_for_sets)
    {
        if (elem.second.source)
        {
            children.push_back(elem.second.source);

            if (elem.second.set)
                elem.second.set->setHeader(elem.second.source->getHeader());
        }
    }

    children.push_back(input);
}


Block CreatingSetsBlockInputStream::readImpl()
{
    Block res;

    createAll();

    if (isCancelledOrThrowIfKilled())
        return res;

    return children.back()->read();
}


void CreatingSetsBlockInputStream::readPrefixImpl()
{
    createAll();
}


Block CreatingSetsBlockInputStream::getTotals()
{
    return children.back()->getTotals();
}


void CreatingSetsBlockInputStream::createAll()
{
    if (!created)
    {
        for (auto & elem : subqueries_for_sets)
        {
            if (elem.second.source) /// There could be prepared in advance Set/Join - no source is specified for them.
            {
                if (isCancelledOrThrowIfKilled())
                    return;

                createOne(elem.second);
            }
        }

        created = true;
    }
}


void CreatingSetsBlockInputStream::createOne(SubqueryForSet & subquery)
{
    LOG_TRACE(log, (subquery.set ? "Creating set. " : "")
        << (subquery.join ? "Creating join. " : "")
        << (subquery.table ? "Filling temporary table. " : ""));
    Stopwatch watch;

    BlockOutputStreamPtr table_out;
    if (subquery.table)
        table_out = subquery.table->write({}, context);

    bool done_with_set = !subquery.set;
    bool done_with_join = !subquery.join;
    bool done_with_table = !subquery.table;

    if (done_with_set && done_with_join && done_with_table)
        throw Exception("Logical error: nothing to do with subquery", ErrorCodes::LOGICAL_ERROR);

    if (table_out)
        table_out->writePrefix();

    while (Block block = subquery.source->read())
    {
        if (isCancelled())
        {
            LOG_DEBUG(log, "Query was cancelled during set / join or temporary table creation.");
            return;
        }

        if (!done_with_set)
        {
            if (!subquery.set->insertFromBlock(block))
                done_with_set = true;
        }

        if (!done_with_join)
        {
            if (!subquery.insertJoinedBlock(block))
                done_with_join = true;
        }

        if (!done_with_table)
        {
            block = materializeBlock(block);
            table_out->write(block);

            rows_to_transfer += block.rows();
            bytes_to_transfer += block.bytes();

            if (!network_transfer_limits.check(rows_to_transfer, bytes_to_transfer, "IN/JOIN external table", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
                done_with_table = true;
        }

        if (done_with_set && done_with_join && done_with_table)
        {
            subquery.source->cancel(false);
            break;
        }
    }

    if (subquery.set)
        subquery.set->finishInsert();

    if (table_out)
        table_out->writeSuffix();

    watch.stop();

    size_t head_rows = 0;
    const BlockStreamProfileInfo & profile_info = subquery.source->getProfileInfo();

    head_rows = profile_info.rows;

    subquery.setTotals();

    if (head_rows != 0)
    {
        std::stringstream msg;
        msg << std::fixed << std::setprecision(3);
        msg << "Created. ";

        if (subquery.set)
            msg << "Set with " << subquery.set->getTotalRowCount() << " entries from " << head_rows << " rows. ";
        if (subquery.join)
            msg << "Join with " << subquery.join->getTotalRowCount() << " entries from " << head_rows << " rows. ";
        if (subquery.table)
            msg << "Table with " << head_rows << " rows. ";

        msg << "In " << watch.elapsedSeconds() << " sec.";
        LOG_DEBUG(log, msg.rdbuf());
    }
    else
    {
        LOG_DEBUG(log, "Subquery has empty result.");
    }
}

}
