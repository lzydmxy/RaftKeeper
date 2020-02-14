#include <Storages/ReadInOrderOptimizer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/AnalyzedJoin.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ReadInOrderOptimizer::ReadInOrderOptimizer(
    const ManyExpressionActions & elements_actions_,
    const SortDescription & required_sort_description_,
    const SyntaxAnalyzerResultPtr & syntax_result)
    : elements_actions(elements_actions_)
    , required_sort_description(required_sort_description_)
{
    if (elements_actions.size() != required_sort_description.size())
        throw Exception("Sizes of sort description and actions are mismatched", ErrorCodes::LOGICAL_ERROR);

    /// Do not analyze joined columns.
    /// They may have aliases and come to description as is.
    /// We can mismatch them with order key columns at stage of fetching columns.
    for (const auto & elem : syntax_result->array_join_result_to_source)
        forbidden_columns.insert(elem.first);
}

InputSortingInfoPtr ReadInOrderOptimizer::getInputOrder(const StoragePtr & storage) const
{
    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(storage.get());
    if (!merge_tree || !merge_tree->hasSortingKey())
        return {};

    SortDescription order_key_prefix_descr;
    int read_direction = required_sort_description.at(0).direction;

    const auto & sorting_key_columns = merge_tree->getSortingKeyColumns();
    size_t prefix_size = std::min(required_sort_description.size(), sorting_key_columns.size());

    for (size_t i = 0; i < prefix_size; ++i)
    {
        if (forbidden_columns.count(required_sort_description[i].column_name))
            break;

        /// Optimize in case of exact match with order key element
        ///  or in some simple cases when order key element is wrapped into monotonic function.
        int current_direction = required_sort_description[i].direction;
        if (required_sort_description[i].column_name == sorting_key_columns[i] && current_direction == read_direction)
            order_key_prefix_descr.push_back(required_sort_description[i]);
        else
        {
            /// Allow only one simple monotonic functions with one argument
            bool found_function = false;
            for (const auto & action : elements_actions[i]->getActions())
            {
                if (action.type != ExpressionAction::APPLY_FUNCTION)
                    continue;

                if (found_function)
                {
                    current_direction = 0;
                    break;
                }
                else
                    found_function = true;

                if (action.argument_names.size() != 1 || action.argument_names.at(0) != sorting_key_columns[i])
                {
                    current_direction = 0;
                    break;
                }

                const auto & func = *action.function_base;
                if (!func.hasInformationAboutMonotonicity())
                {
                    current_direction = 0;
                    break;
                }

                auto monotonicity = func.getMonotonicityForRange(*func.getArgumentTypes().at(0), {}, {});
                if (!monotonicity.is_monotonic)
                {
                    current_direction = 0;
                    break;
                }
                else if (!monotonicity.is_positive)
                    current_direction *= -1;
            }

            if (!found_function)
                current_direction = 0;

            if (!current_direction || (i > 0 && current_direction != read_direction))
                break;

            if (i == 0)
                read_direction = current_direction;

            order_key_prefix_descr.push_back(required_sort_description[i]);
        }
    }

    if (order_key_prefix_descr.empty())
        return {};

    return std::make_shared<InputSortingInfo>(std::move(order_key_prefix_descr), read_direction);
}


AggregateInOrderOptimizer::AggregateInOrderOptimizer(
        const Names & group_by_description_,
        const SyntaxAnalyzerResultPtr & syntax_result)
        : group_by_description(group_by_description_)
{
        /// Not sure yet but let it be
        for (const auto & elem : syntax_result->array_join_result_to_source)
            forbidden_columns.insert(elem.first);
}

GroupByInfoPtr AggregateInOrderOptimizer::getGroupByCommonPrefix(const StoragePtr &storage) const
{
    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(storage.get());
    if (!merge_tree || !merge_tree->hasSortingKey())
        return {};

    Names group_by_common_prefix;
    const auto & sorting_key_columns = merge_tree->getSortingKeyColumns();
    size_t prefix_size = std::min(group_by_description.size(), sorting_key_columns.size());

    for (size_t i = 0; i < prefix_size; ++i)
    {
        if (forbidden_columns.count(group_by_description[i]))
            break;

        if (group_by_description[i] == sorting_key_columns[i])
            group_by_common_prefix.push_back(group_by_description[i]);
        else
        {
            /// TODO injective functions
            break;
        }
    }

    if (group_by_common_prefix.empty())
        return {};

    return std::make_shared<GroupByInfo>(std::move(group_by_common_prefix));
}

}
