#pragma once

#include <DB/Functions/Conditional/CondException.h>
#include <DB/Functions/Conditional/common.h>
#include <DB/Functions/Conditional/CondSource.h>
#include <DB/Functions/NumberTraits.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnConst.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;

}

namespace Conditional
{

namespace
{

/// This class provides type-independent access to the values of a numeric branch
/// (then, else) column. Returned values have the type TResult.
template <typename TResult>
class NumericSource
{
public:
	virtual TResult get(size_t i) const = 0;
	virtual ~NumericSource() = default;
};

template <typename TResult>
using NumericSourcePtr = std::unique_ptr<NumericSource<TResult> >;

template <typename TResult>
using NumericSources = std::vector<NumericSourcePtr<TResult> >;

/// Column type-specific implementation of NumericSource.
template <typename TResult, typename TType, bool IsConst>
class NumericSourceImpl;

template <typename TResult, typename TType>
class NumericSourceImpl<TResult, TType, true> final : public NumericSource<TResult>
{
public:
	NumericSourceImpl(const Block & block, const ColumnNumbers & args, const Branch & br)
	{
		size_t index = br.index;

		const ColumnPtr & col = block.getByPosition(args[index]).column;
		const auto * const_col = typeid_cast<const ColumnConst<TType> *>(&*col);
		if (const_col == nullptr)
			throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
		data = const_col->getData();
	}

	NumericSourceImpl(const NumericSourceImpl &) = delete;
	NumericSourceImpl & operator=(const NumericSourceImpl &) = delete;

	NumericSourceImpl(NumericSourceImpl &&) = default;
	NumericSourceImpl & operator=(NumericSourceImpl &&) = default;

	TResult get(size_t i) const override
	{
		return static_cast<TResult>(data);
	};

private:
	TType data;
};

template <typename TResult, typename TType>
class NumericSourceImpl<TResult, TType, false> final : public NumericSource<TResult>
{
public:
	NumericSourceImpl(const Block & block, const ColumnNumbers & args, const Branch & br)
		: data_array{initDataArray(block, args, br)}
	{
	}

	NumericSourceImpl(const NumericSourceImpl &) = delete;
	NumericSourceImpl & operator=(const NumericSourceImpl &) = delete;

	NumericSourceImpl(NumericSourceImpl &&) = default;
	NumericSourceImpl & operator=(NumericSourceImpl &&) = default;

	TResult get(size_t i) const override
	{
		return static_cast<TResult>(data_array[i]);
	};

private:
	static const PaddedPODArray<TType> & initDataArray(const Block & block,
		const ColumnNumbers & args, const Branch & br)
	{
		size_t index = br.index;
		const ColumnPtr & col = block.getByPosition(args[index]).column;
		const auto * vec_col = typeid_cast<const ColumnVector<TType> *>(&*col);
		if (vec_col == nullptr)
			throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
		return vec_col->getData();
	}

private:
	const PaddedPODArray<TType> & data_array;
};

/// Create a numeric column accessor if TType is the type registered
/// in the specified branch info.
template <typename TResult, typename TType>
class NumericSourceCreator final
{
public:
	static bool execute(NumericSourcePtr<TResult> & source, const Block & block,
		const ColumnNumbers & args, const Branch & br)
	{
		auto type_name = br.type->getName();
		if (TypeName<TType>::get() == type_name)
		{
			if (br.is_const)
				source = std::make_unique<NumericSourceImpl<TResult, TType, true> >(block, args, br);
			else
				source = std::make_unique<NumericSourceImpl<TResult, TType, false> >(block, args, br);
			return true;
		}
		else
			return false;
	}
};

}

/// Processing of multiIf in the case of scalar numeric types.
template <typename TResult>
class NumericEvaluator final
{
public:
	static void perform(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result, size_t tracker)
	{
		const CondSources conds = createConds(block, args);
		const NumericSources<TResult> sources = createNumericSources(block, args, branches);
		size_t row_count = conds[0].getSize();
		PaddedPODArray<TResult> & res = createSink(block, result, row_count);

		ColumnUInt64 * tracker_col = nullptr;
		if (tracker != result)
		{
			auto & col = block.unsafeGetByPosition(tracker).column;
			col = std::make_shared<ColumnUInt64>(row_count);
			tracker_col = static_cast<ColumnUInt64 *>(col.get());
		}

		for (size_t cur_row = 0; cur_row < row_count; ++cur_row)
		{
			bool has_triggered_cond = false;

			size_t cur_source = 0;
			for (const auto & cond : conds)
			{
				if (cond.get(cur_row))
				{
					res[cur_row] = sources[cur_source]->get(cur_row);
					if (tracker_col != nullptr)
					{
						auto & data = tracker_col->getData();
						data[cur_row] = args[branches[cur_source].index];
					}
					has_triggered_cond = true;
					break;
				}
				++cur_source;
			}

			if (!has_triggered_cond)
			{
				res[cur_row] = sources.back()->get(cur_row);
				if (tracker_col != nullptr)
				{
					auto & data = tracker_col->getData();
					data[cur_row] = args[branches.back().index];
				}
			}
		}
	}

private:
	/// Create the result column.
	static PaddedPODArray<TResult> & createSink(Block & block, size_t result, size_t size)
	{
		std::shared_ptr<ColumnVector<TResult>> col_res = std::make_shared<ColumnVector<TResult>>();
		block.getByPosition(result).column = col_res;

		typename ColumnVector<TResult>::Container_t & vec_res = col_res->getData();
		vec_res.resize(size);

		return vec_res;
	}

	/// Create accessors for condition values.
	static CondSources createConds(const Block & block, const ColumnNumbers & args)
	{
		CondSources conds;
		conds.reserve(getCondCount(args));

		for (size_t i = firstCond(); i < elseArg(args); i = nextCond(i))
			conds.emplace_back(block, args, i);
		return conds;
	}

	/// Create accessors for branch values.
	static NumericSources<TResult> createNumericSources(const Block & block,
		const ColumnNumbers & args, const Branches & branches)
	{
		NumericSources<TResult> sources;
		sources.reserve(branches.size());

		for (const auto & br : branches)
		{
			NumericSourcePtr<TResult> source;

			if (! (NumericSourceCreator<TResult, UInt8>::execute(source, block, args, br)
				|| NumericSourceCreator<TResult, UInt16>::execute(source, block, args, br)
				|| NumericSourceCreator<TResult, UInt32>::execute(source, block, args, br)
				|| NumericSourceCreator<TResult, UInt64>::execute(source, block, args, br)
				|| NumericSourceCreator<TResult, Int8>::execute(source, block, args, br)
				|| NumericSourceCreator<TResult, Int16>::execute(source, block, args, br)
				|| NumericSourceCreator<TResult, Int32>::execute(source, block, args, br)
				|| NumericSourceCreator<TResult, Int64>::execute(source, block, args, br)
				|| NumericSourceCreator<TResult, Float32>::execute(source, block, args, br)
				|| NumericSourceCreator<TResult, Float64>::execute(source, block, args, br)
				|| NumericSourceCreator<TResult, Null>::execute(source, block, args, br)))
				throw CondException{CondErrorCodes::NUMERIC_EVALUATOR_ILLEGAL_ARGUMENT, toString(br.index)};

			sources.push_back(std::move(source));
		}

		return sources;
	}
};

/// Processing of multiIf in the case of an invalid return type.
template <>
class NumericEvaluator<NumberTraits::Error>
{
public:
	/// The tracker parameter is an index to a column that tracks the originating column of each value of
	/// the result column. Calling this function with result == tracker means that no such tracking is
	/// required, which happens if multiIf is called with no nullable parameters.
	static void perform(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result, size_t tracker)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

}

}
