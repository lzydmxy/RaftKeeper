#pragma once

#include <DB/Core/NamesAndTypes.h>
#include <DB/Storages/MergeTree/RangesInDataPart.h>
#include <statdaemons/ext/range.hpp>
#include <mutex>


namespace DB
{


struct MergeTreeReadTask
{
	MergeTreeData::DataPartPtr data_part;
	MarkRanges mark_ranges;
	std::size_t part_index_in_query;
	const Names & ordered_names;
	const NameSet & column_name_set;
	const NamesAndTypesList & columns;
	const NamesAndTypesList & pre_columns;
	const bool remove_prewhere_column;
	const bool should_reorder;

	MergeTreeReadTask(
		const MergeTreeData::DataPartPtr & data_part, const MarkRanges & ranges, const std::size_t part_index_in_query,
		const Names & ordered_names, const NameSet & column_name_set, const NamesAndTypesList & columns,
		const NamesAndTypesList & pre_columns, const bool remove_prewhere_column, const bool should_reorder)
		: data_part{data_part}, mark_ranges{ranges}, part_index_in_query{part_index_in_query},
		  ordered_names{ordered_names}, column_name_set{column_name_set}, columns{columns}, pre_columns{pre_columns},
		  remove_prewhere_column{remove_prewhere_column}, should_reorder{should_reorder}
	{}
};

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;

class MergeTreeReadPool
{
	std::size_t threads;
public:
	MergeTreeReadPool(
		const std::size_t threads,
		const RangesInDataParts & parts, MergeTreeData & data, const ExpressionActionsPtr & prewhere_actions,
		const String & prewhere_column_name, const bool check_columns, const Names & column_names)
		: threads{threads}, parts{parts}, data{data}, column_names{column_names}
	{
		fillPerPartInfo(prewhere_actions, prewhere_column_name, check_columns);
	}

	MergeTreeReadPool(const MergeTreeReadPool &) = delete;
	MergeTreeReadPool & operator=(const MergeTreeReadPool &) = delete;

	MergeTreeReadTaskPtr getTask(const std::size_t min_marks_to_read, const std::size_t thread)
	{
		const std::lock_guard<std::mutex> lock{mutex};

		if (remaining_part_indices.empty())
			return nullptr;

		const auto idx = remaining_part_indices.size() - (1 + remaining_part_indices.size() * thread / threads);
		/// find a part which has marks remaining
//		const auto part_id = remaining_part_indices.back();
		const auto part_id = remaining_part_indices[idx];

		auto & part = parts[part_id];
		const auto & column_name_set = per_part_column_name_set[part_id];
		const auto & columns = per_part_columns[part_id];
		const auto & pre_columns = per_part_pre_columns[part_id];
		const auto remove_prewhere_column = per_part_remove_prewhere_column[part_id];
		auto & marks_in_part = per_part_sum_marks[part_id];

		/// Берём весь кусок, если он достаточно мал
		auto need_marks = std::min(marks_in_part, min_marks_to_read);

		/// Не будем оставлять в куске слишком мало строк.
		if (marks_in_part > need_marks &&
			marks_in_part - need_marks < min_marks_to_read)
			need_marks = marks_in_part;

		MarkRanges ranges_to_get_from_part;

		/// Возьмем весь кусок, если он достаточно мал.
		if (marks_in_part <= need_marks)
		{
			const auto marks_to_get_from_range = marks_in_part;

			/// Восстановим порядок отрезков.
			std::reverse(part.ranges.begin(), part.ranges.end());

			ranges_to_get_from_part = part.ranges;

			marks_in_part -= marks_to_get_from_range;

			std::swap(remaining_part_indices[idx], remaining_part_indices.back());
			remaining_part_indices.pop_back();
		}
		else
		{
			/// Цикл по отрезкам куска.
			while (need_marks > 0 && !part.ranges.empty())
			{
				auto & range = part.ranges.back();

				const std::size_t marks_in_range = range.end - range.begin;
				const std::size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

				ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
				range.begin += marks_to_get_from_range;
				if (range.begin == range.end)
					part.ranges.pop_back();

				marks_in_part -= marks_to_get_from_range;
				need_marks -= marks_to_get_from_range;
			}

			if (0 == marks_in_part)
			{
				std::swap(remaining_part_indices[idx], remaining_part_indices.back());
				remaining_part_indices.pop_back();
			}
		}

		return std::make_unique<MergeTreeReadTask>(
			part.data_part, ranges_to_get_from_part, part.part_index_in_query, column_names, column_name_set, columns,
			pre_columns, remove_prewhere_column, per_part_should_reorder[part_id]);
	}

public:
	void fillPerPartInfo(
		const ExpressionActionsPtr & prewhere_actions, const String & prewhere_column_name, const bool check_columns)
	{
		remaining_part_indices.reserve(parts.size());

		for (const auto i : ext::range(0, parts.size()))
		{
			auto & part = parts[i];

			/// Посчитаем засечки для каждого куска.
			size_t sum_marks = 0;
			/// Пусть отрезки будут перечислены справа налево, чтобы можно было выбрасывать самый левый отрезок с помощью pop_back().
			std::reverse(std::begin(part.ranges), std::end(part.ranges));

			for (const auto & range : part.ranges)
				sum_marks += range.end - range.begin;

			per_part_sum_marks.push_back(sum_marks);

			if (0 != sum_marks)
				remaining_part_indices.push_back(i);

			per_part_columns_lock.push_back(std::make_unique<Poco::ScopedReadRWLock>(
				part.data_part->columns_lock));

			/// inject column names required for DEFAULT evaluation in current part
			auto required_column_names = column_names;

			const auto injected_columns = injectRequiredColumns(part.data_part, required_column_names);
			auto should_reoder = !injected_columns.empty();

			Names required_pre_column_names;

			if (prewhere_actions)
			{
				/// collect columns required for PREWHERE evaluation
				required_pre_column_names = prewhere_actions->getRequiredColumns();

				/// there must be at least one column required for PREWHERE
				if (required_pre_column_names.empty())
					required_pre_column_names.push_back(required_column_names[0]);

				/// PREWHERE columns may require some additional columns for DEFAULT evaluation
				const auto injected_pre_columns = injectRequiredColumns(part.data_part, required_pre_column_names);
				if (!injected_pre_columns.empty())
					should_reoder = true;

				/// will be used to distinguish between PREWHERE and WHERE columns when applying filter
				const NameSet pre_name_set{
					std::begin(required_pre_column_names), std::end(required_pre_column_names)
				};
				/** Если выражение в PREWHERE - не столбец таблицы, не нужно отдавать наружу столбец с ним
				 *	(от storage ожидают получить только столбцы таблицы). */
				per_part_remove_prewhere_column.push_back(0 == pre_name_set.count(prewhere_column_name));

				Names post_column_names;
				for (const auto & name : required_column_names)
					if (!pre_name_set.count(name))
						post_column_names.push_back(name);

				required_column_names = post_column_names;
			}
			else
				per_part_remove_prewhere_column.push_back(false);

			per_part_column_name_set.emplace_back(std::begin(required_column_names), std::end(required_column_names));

			if (check_columns)
			{
				/** Под part->columns_lock проверим, что все запрошенные столбцы в куске того же типа, что в таблице.
				 *	Это может быть не так во время ALTER MODIFY. */
				if (!required_pre_column_names.empty())
					data.check(part.data_part->columns, required_pre_column_names);
				if (!required_column_names.empty())
					data.check(part.data_part->columns, required_column_names);

				per_part_pre_columns.push_back(data.getColumnsList().addTypes(required_pre_column_names));
				per_part_columns.push_back(data.getColumnsList().addTypes(required_column_names));
			}
			else
			{
				per_part_pre_columns.push_back(part.data_part->columns.addTypes(required_pre_column_names));
				per_part_columns.push_back(part.data_part->columns.addTypes(required_column_names));
			}

			per_part_should_reorder.push_back(should_reoder);
		}
	}

	/** Если некоторых запрошенных столбцов нет в куске,
	 *	то выясняем, какие столбцы может быть необходимо дополнительно прочитать,
	 *	чтобы можно было вычислить DEFAULT выражение для этих столбцов.
	 *	Добавляет их в columns. */
	NameSet injectRequiredColumns(const MergeTreeData::DataPartPtr & part, Names & columns) const
	{
		NameSet required_columns{std::begin(columns), std::end(columns)};
		NameSet injected_columns;

		auto all_column_files_missing = true;

		for (size_t i = 0; i < columns.size(); ++i)
		{
			const auto & column_name = columns[i];

			/// column has files and hence does not require evaluation
			if (part->hasColumnFiles(column_name))
			{
				all_column_files_missing = false;
				continue;
			}

			const auto default_it = data.column_defaults.find(column_name);
			/// columns has no explicit default expression
			if (default_it == std::end(data.column_defaults))
				continue;

			/// collect identifiers required for evaluation
			IdentifierNameSet identifiers;
			default_it->second.expression->collectIdentifierNames(identifiers);

			for (const auto & identifier : identifiers)
			{
				if (data.hasColumn(identifier))
				{
					/// ensure each column is added only once
					if (required_columns.count(identifier) == 0)
					{
						columns.emplace_back(identifier);
						required_columns.emplace(identifier);
						injected_columns.emplace(identifier);
					}
				}
			}
		}

		if (all_column_files_missing)
		{
			addMinimumSizeColumn(part, columns);
			/// correctly report added column
			injected_columns.insert(columns.back());
		}

		return injected_columns;
	}

	/** Добавить столбец минимального размера.
	  * Используется в случае, когда ни один столбец не нужен, но нужно хотя бы знать количество строк.
	  * Добавляет в columns.
	  */
	void addMinimumSizeColumn(const MergeTreeData::DataPartPtr & part, Names & columns) const
	{
		const auto get_column_size = [this, &part] (const String & name) {
			const auto & files = part->checksums.files;

			const auto escaped_name = escapeForFileName(name);
			const auto bin_file_name = escaped_name + ".bin";
			const auto mrk_file_name = escaped_name + ".mrk";

			return files.find(bin_file_name)->second.file_size + files.find(mrk_file_name)->second.file_size;
		};

		const auto & storage_columns = data.getColumnsList();
		const NameAndTypePair * minimum_size_column = nullptr;
		auto minimum_size = std::numeric_limits<size_t>::max();

		for (const auto & column : storage_columns)
		{
			if (!part->hasColumnFiles(column.name))
				continue;

			const auto size = get_column_size(column.name);
			if (size < minimum_size)
			{
				minimum_size = size;
				minimum_size_column = &column;
			}
		}

		if (!minimum_size_column)
			throw Exception{
				"Could not find a column of minimum size in MergeTree",
				ErrorCodes::LOGICAL_ERROR
			};

		columns.push_back(minimum_size_column->name);
	}

	std::vector<std::unique_ptr<Poco::ScopedReadRWLock>> per_part_columns_lock;
	RangesInDataParts parts;
	std::vector<std::size_t> per_part_sum_marks;
	std::vector<std::size_t> remaining_part_indices;
	MergeTreeData & data;
	Names column_names;
	std::vector<NameSet> per_part_column_name_set;
	std::vector<NamesAndTypesList> per_part_columns;
	std::vector<NamesAndTypesList> per_part_pre_columns;
	/// @todo actually all of these values are either true or false for the whole query, thus no vector required
	std::vector<bool> per_part_remove_prewhere_column;
	std::vector<bool> per_part_should_reorder;

	mutable std::mutex mutex;
};

using MergeTreeReadPoolPtr = std::shared_ptr<MergeTreeReadPool>;


}
