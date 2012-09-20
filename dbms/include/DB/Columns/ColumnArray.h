#pragma once

#include <string.h> // memcpy

#include <Poco/SharedPtr.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/IColumn.h>
#include <DB/Columns/ColumnsNumber.h>


namespace DB
{

using Poco::SharedPtr;

/** Cтолбeц значений типа массив.
  * В памяти он представлен, как один столбец вложенного типа, размер которого равен сумме размеров всех массивов,
  *  и как массив смещений в нём, который позволяет достать каждый элемент.
  */
class ColumnArray : public IColumn
{
public:
	/** По индексу i находится смещение до начала i + 1 -го элемента. */
	typedef ColumnVector<Offset_t> ColumnOffsets_t;

	/** Создать пустой столбец массивов, с типом значений, как в столбце nested_column */
	ColumnArray(ColumnPtr nested_column)
		: data(nested_column), offsets(new ColumnOffsets_t)
	{
		data->clear();
	}

	std::string getName() const { return "ColumnArray(" + data->getName() + ")"; }

	ColumnPtr cloneEmpty() const
	{
		return new ColumnArray(data->cloneEmpty());
	}
	
	size_t size() const
	{
		return getOffsets().size();
	}
	
	Field operator[](size_t n) const
	{
		size_t offset = offsetAt(n);
		size_t size = sizeAt(n);
		Array res(size);
		
		for (size_t i = 0; i < size; ++i)
			res[i] = (*data)[offset + i];
			
		return res;
	}

	void cut(size_t start, size_t length)
	{
		if (length == 0)
		{
			data->cut(0, 0);
			Offsets_t().swap(getOffsets());
			return;
		}
		
		if (start + length > getOffsets().size())
			throw Exception("Parameter out of bound in IColumnArray::cut() method.",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		size_t nested_offset = offsetAt(start);
		size_t nested_length = getOffsets()[start + length - 1] - nested_offset;

		data->cut(nested_offset, nested_length);

		if (start == 0)
			getOffsets().resize(length);
		else
		{
			Offsets_t tmp(length);

			for (size_t i = 0; i < length; ++i)
				tmp[i] = getOffsets()[start + i] - nested_offset;
			
			tmp.swap(getOffsets());
		}
	}

	void insert(const Field & x)
	{
		const Array & array = boost::get<const Array &>(x);
		size_t size = array.size();
		for (size_t i = 0; i < size; ++i)
			data->insert(array[i]);
		getOffsets().push_back((getOffsets().size() == 0 ? 0 : getOffsets().back()) + size);
	}

	void insertDefault()
	{
		data->insertDefault();
		getOffsets().push_back(getOffsets().size() == 0 ? 1 : (getOffsets().back() + 1));
	}

	void clear()
	{
		data->clear();
		getOffsets().clear();
	}

	void filter(const Filter & filt)
	{
		size_t size = getOffsets().size();
		if (size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return;

		/// Не слишком оптимально. Можно сделать специализацию для массивов известных типов.
		Filter nested_filt(getOffsets().back());
		for (size_t i = 0; i < size; ++i)
			if (filt[i])
				memset(&nested_filt[offsetAt(i)], 1, sizeAt(i));
		data->filter(nested_filt);
				
		Offsets_t tmp;
		tmp.reserve(size);

		size_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			if (filt[i])
			{
				current_offset += sizeAt(i);
				tmp.push_back(current_offset);
			}
		}

		tmp.swap(getOffsets());
	}

	void replicate(const Offsets_t & offsets)
	{
		throw Exception("Replication of column Array is not implemented.", ErrorCodes::NOT_IMPLEMENTED);
	}

	void permute(const Permutation & perm)
	{
		size_t size = getOffsets().size();
		if (size != perm.size())
			throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return;

		Permutation nested_perm(getOffsets().back());

		Offsets_t tmp_offsets(size);
		size_t current_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			for (size_t j = 0; j < sizeAt(perm[i]); ++j)
				nested_perm[current_offset + j] = offsetAt(perm[i]) + j;
			current_offset += sizeAt(perm[i]);
			tmp_offsets[i] = current_offset;
		}

		data->permute(nested_perm);
		tmp_offsets.swap(getOffsets());
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_) const
	{
		const ColumnArray & rhs = static_cast<const ColumnArray &>(rhs_);

		/// Не оптимально
		size_t lhs_size = sizeAt(n);
		size_t rhs_size = rhs.sizeAt(m);
		size_t min_size = std::min(lhs_size, rhs_size);
		for (size_t i = 0; i < min_size; ++i)
			if (int res = data->compareAt(offsetAt(n) + i, rhs.offsetAt(m) + i, *rhs.data))
				return res;

		return lhs_size < rhs_size
			? -1
			: (lhs_size == rhs_size
				? 0
				: 1);
	}

	struct less
	{
		const ColumnArray & parent;
		const Permutation & nested_perm;

		less(const ColumnArray & parent_, const Permutation & nested_perm_) : parent(parent_), nested_perm(nested_perm_) {}
		bool operator()(size_t lhs, size_t rhs) const
		{
			size_t lhs_size = parent.sizeAt(lhs);
			size_t rhs_size = parent.sizeAt(rhs);
			size_t min_size = std::min(lhs_size, rhs_size);
			for (size_t i = 0; i < min_size; ++i)
			{
				if (nested_perm[parent.offsetAt(lhs) + i] < nested_perm[parent.offsetAt(rhs) + i])
					return true;
				else if (nested_perm[parent.offsetAt(lhs) + i] > nested_perm[parent.offsetAt(rhs) + i])
					return false;
			}
			return lhs_size < rhs_size;
		}
	};

	Permutation getPermutation() const
	{
		Permutation nested_perm = data->getPermutation();
		size_t s = size();
		Permutation res(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;

		std::sort(res.begin(), res.end(), less(*this, nested_perm));
		return res;
	}

	size_t byteSize() const
	{
		return data->byteSize() + getOffsets().size() * sizeof(getOffsets()[0]);
	}

	/** Более эффективные методы манипуляции */
	IColumn & getData() { return *data; }
	const IColumn & getData() const { return *data; }

	ColumnPtr & getDataPtr() { return data; }
	const ColumnPtr & getDataPtr() const { return data; }

	Offsets_t & __attribute__((__flatten__, __always_inline__)) getOffsets()
	{
		return static_cast<ColumnOffsets_t &>(*offsets.get()).getData();
	}

	const Offsets_t & __attribute__((__flatten__, __always_inline__)) getOffsets() const
	{
		return static_cast<const ColumnOffsets_t &>(*offsets.get()).getData();
	}

	ColumnPtr & getOffsetsColumn() { return offsets; }
	const ColumnPtr & getOffsetsColumn() const { return offsets; }

protected:
	ColumnPtr data;
	ColumnPtr offsets;	/// Смещения могут быть разделяемыми для нескольких столбцов - для реализации вложенных структур данных.

	size_t __attribute__((__flatten__, __always_inline__)) offsetAt(size_t i) const { return i == 0 ? 0 : getOffsets()[i - 1]; }
	size_t __attribute__((__flatten__, __always_inline__)) sizeAt(size_t i) const	{ return i == 0 ? getOffsets()[0] : (getOffsets()[i] - getOffsets()[i - 1]); }
};


}
