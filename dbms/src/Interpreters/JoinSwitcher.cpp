#include <Common/typeid_cast.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/Join.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/join_common.h>

namespace DB
{

static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable)
{
    if (nullable)
        JoinCommon::convertColumnToNullable(column);
    else
        JoinCommon::removeColumnNullability(column);

    return std::move(column);
}

JoinSwitcher::JoinSwitcher(std::shared_ptr<AnalyzedJoin> table_join_, const Block & right_sample_block_)
    : switched(false)
    , limits(table_join_->sizeLimits())
    , table_join(table_join_)
    , right_sample_block(right_sample_block_.cloneEmpty())
{
    join = std::make_shared<Join>(table_join, right_sample_block);

    if (!limits.hasLimits())
        limits.max_bytes = table_join->defaultMaxBytes();
}

bool JoinSwitcher::addJoinedBlock(const Block & block, bool)
{
    /// Trying to make MergeJoin without lock

    if (switched)
        return join->addJoinedBlock(block);

    std::lock_guard lock(switch_mutex);

    if (switched)
        return join->addJoinedBlock(block);

    /// HashJoin with external limits check

    join->addJoinedBlock(block, false);
    size_t rows = join->getTotalRowCount();
    size_t bytes = join->getTotalByteCount();

    if (!limits.softCheck(rows, bytes))
        switchJoin();

    return true;
}

void JoinSwitcher::switchJoin()
{
    std::shared_ptr<Join::RightTableData> joined_data = static_cast<const Join &>(*join).getJoinedData();
    BlocksList right_blocks = std::move(joined_data->blocks);

    /// Destroy old join & create new one. Destroy first in case of memory saving.
    join = std::make_shared<MergeJoin>(table_join, right_sample_block);

    /// names to positions optimization
    std::vector<size_t> positions;
    std::vector<bool> is_nullable;
    if (right_blocks.size())
    {
        positions.reserve(right_sample_block.columns());
        const Block & tmp_block = *right_blocks.begin();
        for (const auto & sample_column : right_sample_block)
        {
            positions.emplace_back(tmp_block.getPositionByName(sample_column.name));
            is_nullable.emplace_back(sample_column.type->isNullable());
        }
    }

    for (Block & saved_block : right_blocks)
    {
        Block restored_block;
        for (size_t i = 0; i < positions.size(); ++i)
        {
            auto & column = saved_block.getByPosition(positions[i]);
            restored_block.insert(correctNullability(std::move(column), is_nullable[i]));
        }
        join->addJoinedBlock(restored_block);
    }

    switched = true;
}

}
