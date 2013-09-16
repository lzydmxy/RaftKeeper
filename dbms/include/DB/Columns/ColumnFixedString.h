#pragma once

#include <string.h> // memcpy

#include <DB/Common/PODArray.h>
#include <DB/Columns/IColumn.h>


namespace DB
{

/** Cтолбeц значений типа "строка фиксированной длины".
  * Если вставить строку меньшей длины, то она будет дополнена нулевыми байтами.
  */
class ColumnFixedString : public IColumn
{
public:
	//typedef std::vector<UInt8> Chars_t;
	typedef PODArray<UInt8> Chars_t;

private:
	/// Размер строк.
	const size_t n;
	/// Байты строк, уложенные подряд. Строки хранятся без завершающего нулевого байта.
	Chars_t chars;

public:
	/** Создать пустой столбец строк фиксированной длины n */
	ColumnFixedString(size_t n_) : n(n_) {}

	std::string getName() const { return "ColumnFixedString"; }

	ColumnPtr cloneEmpty() const
	{
		return new ColumnFixedString(n);
	}

	size_t size() const
	{
		return chars.size() / n;
	}

	size_t sizeOfField() const
	{
		return n;
	}

	bool isFixed() const
	{
		return true;
	}
	
	size_t byteSize() const
	{
		return chars.size() + sizeof(n);
	}
	
	Field operator[](size_t index) const
	{
		return String(reinterpret_cast<const char *>(&chars[n * index]), n);
	}

	void get(size_t index, Field & res) const
	{
		res.assignString(reinterpret_cast<const char *>(&chars[n * index]), n);
	}

	StringRef getDataAt(size_t index) const
	{
		return StringRef(&chars[n * index], n);
	}

	void insert(const Field & x)
	{
		const String & s = DB::get<const String &>(x);

		if (s.size() > n)
			throw Exception("Too large string '" + s + "' for FixedString column", ErrorCodes::TOO_LARGE_STRING_SIZE);
		
		size_t old_size = chars.size();
		chars.resize(old_size + n);
		memcpy(&chars[old_size], s.data(), s.size());
	}

	void insertFrom(const IColumn & src_, size_t index)
	{
		const ColumnFixedString & src = static_cast<const ColumnFixedString &>(src_);

		if (n != src.getN())
			throw Exception("Size of FixedString doesn't match", ErrorCodes::SIZE_OF_ARRAY_DOESNT_MATCH_SIZE_OF_FIXEDARRAY_COLUMN);

		size_t old_size = chars.size();
		chars.resize(old_size + n);
		memcpy(&chars[old_size], &src.chars[n * index], n);
	}

	void insertData(const char * pos, size_t length)
	{
		size_t old_size = chars.size();
		chars.resize(old_size + n);
		memcpy(&chars[old_size], pos, n);
	}

	void insertDefault()
	{
		chars.resize(chars.size() + n);
	}

	int compareAt(size_t p1, size_t p2, const IColumn & rhs_) const
	{
		const ColumnFixedString & rhs = static_cast<const ColumnFixedString &>(rhs_);
		return memcmp(&chars[p1 * n], &rhs.chars[p2 * n], n);
	}

	template <bool positive>
	struct less
	{
		const ColumnFixedString & parent;
		less(const ColumnFixedString & parent_) : parent(parent_) {}
		bool operator()(size_t lhs, size_t rhs) const
		{
			/// TODO: memcmp тормозит.
			return positive == (0 > memcmp(&parent.chars[lhs * parent.n], &parent.chars[rhs * parent.n], parent.n));
		}
	};

	Permutation getPermutation(bool reverse, size_t limit) const
	{
		size_t s = size();
		Permutation res(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;

		if (limit > s)
			limit = 0;

		if (limit)
		{
			if (reverse)
				std::partial_sort(res.begin(), res.begin() + limit, res.end(), less<false>(*this));
			else
				std::partial_sort(res.begin(), res.begin() + limit, res.end(), less<true>(*this));
		}
		else
		{
			if (reverse)
				std::sort(res.begin(), res.end(), less<false>(*this));
			else
				std::sort(res.begin(), res.end(), less<true>(*this));
		}

		return res;
	}

	ColumnPtr cut(size_t start, size_t length) const
	{
		ColumnFixedString * res_ = new ColumnFixedString(n);
		ColumnPtr res = res_;
		res_->chars.resize(n * length);
		memcpy(&res_->chars[0], &chars[n * start], n * length);
		return res;
	}

	ColumnPtr filter(const IColumn::Filter & filt) const
	{
		size_t col_size = size();
		if (col_size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnFixedString * res_ = new ColumnFixedString(n);
		ColumnPtr res = res_;
		res_->chars.reserve(chars.size());

		size_t offset = 0;
		for (size_t i = 0; i < col_size; ++i, offset += n)
		{
			if (filt[i])
			{
				res_->chars.resize(res_->chars.size() + n);
				memcpy(&res_->chars[res_->chars.size() - n], &chars[offset], n);
			}
		}

		return res;
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const
	{
		size_t col_size = size();

		if (limit == 0)
			limit = col_size;
		else
			limit = std::min(col_size, limit);

		if (perm.size() < limit)
			throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (limit == 0)
			return new ColumnFixedString(n);

		ColumnFixedString * res_ = new ColumnFixedString(n);
		ColumnPtr res = res_;

		Chars_t & res_chars = res_->chars;

		res_chars.resize(n * limit);

		size_t offset = 0;
		for (size_t i = 0; i < limit; ++i, offset += n)
			memcpy(&res_chars[offset], &chars[perm[i] * n], n);

		return res;
	}

	ColumnPtr replicate(const Offsets_t & offsets) const
	{
		size_t col_size = size();
		if (col_size != offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnFixedString * res_ = new ColumnFixedString(n);
		ColumnPtr res = res_;
		
		Chars_t & res_chars = res_->chars;
		res_chars.reserve(n * offsets.back());

		Offset_t prev_offset = 0;
		for (size_t i = 0; i < col_size; ++i)
		{
			size_t size_to_replicate = offsets[i] - prev_offset;
			prev_offset = offsets[i];

			for (size_t j = 0; j < size_to_replicate; ++j)
				for (size_t k = 0; k < n; ++k)
					res_chars.push_back(chars[i * n + k]);
		}

		return res;
	}

	void getExtremes(Field & min, Field & max) const
	{
		min = String();
		max = String();
	}


	Chars_t & getChars() { return chars; }
	const Chars_t & getChars() const { return chars; }

	size_t getN() const { return n; }
};


}
