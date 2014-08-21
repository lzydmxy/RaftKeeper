#pragma once

#include <string.h>

#include <DB/Core/Defines.h>

#include <DB/Columns/IColumn.h>
#include <DB/Common/Collator.h>
#include <DB/Common/PODArray.h>


namespace DB
{

/** Cтолбeц значений типа "строка".
  */
class ColumnString final : public IColumn
{
public:
	//typedef std::vector<UInt8> Chars_t;
	typedef PODArray<UInt8> Chars_t;

private:
	/// По индексу i находится смещение до начала i + 1 -го элемента.
	Offsets_t offsets;

	/// Байты строк, уложенные подряд. Строки хранятся с завершающим нулевым байтом.
	Chars_t chars;

	size_t __attribute__((__always_inline__)) offsetAt(size_t i) const	{ return i == 0 ? 0 : offsets[i - 1]; }

	/// Размер, включая завершающий нулевой байт.
	size_t __attribute__((__always_inline__)) sizeAt(size_t i) const	{ return i == 0 ? offsets[0] : (offsets[i] - offsets[i - 1]); }

public:
	/** Создать пустой столбец строк */
	ColumnString() {}

	std::string getName() const { return "ColumnString"; }

	size_t size() const
	{
		return offsets.size();
	}

	size_t byteSize() const
	{
		return chars.size() + offsets.size() * sizeof(offsets[0]);
	}

	ColumnPtr cloneEmpty() const
	{
		return new ColumnString;
	}

	Field operator[](size_t n) const
	{
		return Field(&chars[offsetAt(n)], sizeAt(n) - 1);
	}

	void get(size_t n, Field & res) const
	{
		res.assignString(&chars[offsetAt(n)], sizeAt(n) - 1);
	}

	StringRef getDataAt(size_t n) const
	{
		return StringRef(&chars[offsetAt(n)], sizeAt(n) - 1);
	}

	StringRef getDataAtWithTerminatingZero(size_t n) const
	{
		return StringRef(&chars[offsetAt(n)], sizeAt(n));
	}

	void insert(const Field & x)
	{
		const String & s = DB::get<const String &>(x);
		size_t old_size = chars.size();
		size_t size_to_append = s.size() + 1;

		chars.resize(old_size + size_to_append);
		memcpy(&chars[old_size], s.c_str(), size_to_append);
		offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + size_to_append);
	}

	void insertFrom(const IColumn & src_, size_t n)
	{
		const ColumnString & src = static_cast<const ColumnString &>(src_);
		size_t old_size = chars.size();
		size_t size_to_append = src.sizeAt(n);
		size_t offset = src.offsetAt(n);

		chars.resize(old_size + size_to_append);
		memcpy(&chars[old_size], &src.chars[offset], size_to_append);
		offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + size_to_append);
	}

	void insertData(const char * pos, size_t length)
	{
		size_t old_size = chars.size();

		chars.resize(old_size + length + 1);
		memcpy(&chars[old_size], pos, length);
		chars[old_size + length] = 0;
		offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + length + 1);
	}

	void insertDataWithTerminatingZero(const char * pos, size_t length)
	{
		size_t old_size = chars.size();

		chars.resize(old_size + length);
		memcpy(&chars[old_size], pos, length);
		offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + length);
	}

	ColumnPtr cut(size_t start, size_t length) const
	{
		if (length == 0)
			return new ColumnString;

		if (start + length > offsets.size())
			throw Exception("Parameter out of bound in IColumnString::cut() method.",
				ErrorCodes::PARAMETER_OUT_OF_BOUND);

		size_t nested_offset = offsetAt(start);
		size_t nested_length = offsets[start + length - 1] - nested_offset;

		ColumnString * res_ = new ColumnString;
		ColumnPtr res = res_;

		res_->chars.resize(nested_length);
		memcpy(&res_->chars[0], &chars[nested_offset], nested_length);

		Offsets_t & res_offsets = res_->offsets;

		if (start == 0)
		{
			res_offsets.assign(offsets.begin(), offsets.begin() + length);
		}
		else
		{
			res_offsets.resize(length);

			for (size_t i = 0; i < length; ++i)
				res_offsets[i] = offsets[start + i] - nested_offset;
		}

		return res;
	}

	ColumnPtr filter(const Filter & filt) const
	{
		size_t size = offsets.size();
		if (size != filt.size())
			throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (size == 0)
			return new ColumnString;

		ColumnString * res_ = new ColumnString;
		ColumnPtr res = res_;

		Chars_t & res_chars = res_->chars;
		Offsets_t & res_offsets = res_->offsets;
		res_chars.reserve(chars.size());
		res_offsets.reserve(size);

		Offset_t current_new_offset = 0;

		for (size_t i = 0; i < size; ++i)
		{
			if (!filt[i])
				continue;

			size_t string_offset = i == 0 ? 0 : offsets[i - 1];
			size_t string_size = offsets[i] - string_offset;

			current_new_offset += string_size;
			res_offsets.push_back(current_new_offset);

			res_chars.resize(res_chars.size() + string_size);
			memcpy(&res_chars[res_chars.size() - string_size], &chars[string_offset], string_size);
		}

		return res;
	}

	ColumnPtr permute(const Permutation & perm, size_t limit) const
	{
		size_t size = offsets.size();

		if (limit == 0)
			limit = size;
		else
			limit = std::min(size, limit);

		if (perm.size() < limit)
			throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		if (limit == 0)
			return new ColumnString;

		ColumnString * res_ = new ColumnString;
		ColumnPtr res = res_;

		Chars_t & res_chars = res_->chars;
		Offsets_t & res_offsets = res_->offsets;

		res_chars.resize(chars.size());
		res_offsets.resize(limit);

		Offset_t current_new_offset = 0;

		for (size_t i = 0; i < limit; ++i)
		{
			size_t j = perm[i];
			size_t string_offset = j == 0 ? 0 : offsets[j - 1];
			size_t string_size = offsets[j] - string_offset;

			memcpy(&res_chars[current_new_offset], &chars[string_offset], string_size);

			current_new_offset += string_size;
			res_offsets[i] = current_new_offset;
		}

		res_chars.resize(res_offsets[limit - 1]);

		return res;
	}

	void insertDefault()
	{
		chars.push_back(0);
		offsets.push_back(offsets.size() == 0 ? 1 : (offsets.back() + 1));
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const
	{
		const ColumnString & rhs = static_cast<const ColumnString &>(rhs_);

		/** Для производительности, строки сравниваются до первого нулевого байта.
		  * (если нулевой байт в середине строки, то то, что после него - игнорируется)
		  * Замечу, что завершающий нулевой байт всегда есть.
		  */
		return strcmp(
			reinterpret_cast<const char *>(&chars[offsetAt(n)]),
			reinterpret_cast<const char *>(&rhs.chars[rhs.offsetAt(m)]));
	}

	/// Версия compareAt для locale-sensitive сравнения строк
	int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs_, const Collator & collator) const
	{
		const ColumnString & rhs = static_cast<const ColumnString &>(rhs_);

		return collator.compare(
			reinterpret_cast<const char *>(&chars[offsetAt(n)]), sizeAt(n),
			reinterpret_cast<const char *>(&rhs.chars[rhs.offsetAt(m)]), rhs.sizeAt(m));
	}

	template <bool positive>
	struct less
	{
		const ColumnString & parent;
		less(const ColumnString & parent_) : parent(parent_) {}
		bool operator()(size_t lhs, size_t rhs) const
		{
			int res = strcmp(
				reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(lhs)]),
				reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(rhs)]));

			return positive ? (res < 0) : (res > 0);
		}
	};

	void getPermutation(bool reverse, size_t limit, Permutation & res) const
	{
		size_t s = offsets.size();
		res.resize(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;

		if (limit >= s)
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
	}

	template <bool positive>
	struct lessWithCollation
	{
		const ColumnString & parent;
		const Collator & collator;

		lessWithCollation(const ColumnString & parent_, const Collator & collator_) : parent(parent_), collator(collator_) {}

		bool operator()(size_t lhs, size_t rhs) const
		{
			int res = collator.compare(
				reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(lhs)]), parent.sizeAt(lhs),
				reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(rhs)]), parent.sizeAt(rhs));

			return positive ? (res < 0) : (res > 0);
		}
	};

	/// Сортировка с учетом Collation
	void getPermutationWithCollation(const Collator & collator, bool reverse, size_t limit, Permutation & res) const
	{
		size_t s = offsets.size();
		res.resize(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;

		if (limit >= s)
			limit = 0;

		if (limit)
		{
			if (reverse)
				std::partial_sort(res.begin(), res.begin() + limit, res.end(), lessWithCollation<false>(*this, collator));
			else
				std::partial_sort(res.begin(), res.begin() + limit, res.end(), lessWithCollation<true>(*this, collator));
		}
		else
		{
			if (reverse)
				std::sort(res.begin(), res.end(), lessWithCollation<false>(*this, collator));
			else
				std::sort(res.begin(), res.end(), lessWithCollation<true>(*this, collator));
		}
	}

	ColumnPtr replicate(const Offsets_t & replicate_offsets) const
	{
		size_t col_size = size();
		if (col_size != replicate_offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

		ColumnString * res_ = new ColumnString;
		ColumnPtr res = res_;

		if (0 == col_size)
			return res;

		Chars_t & res_chars = res_->chars;
		Offsets_t & res_offsets = res_->offsets;
		res_chars.reserve(chars.size() / col_size * replicate_offsets.back());
		res_offsets.reserve(replicate_offsets.back());

		Offset_t prev_replicate_offset = 0;
		Offset_t prev_string_offset = 0;
		Offset_t current_new_offset = 0;

		for (size_t i = 0; i < col_size; ++i)
		{
			size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
			size_t string_size = offsets[i] - prev_string_offset;

			for (size_t j = 0; j < size_to_replicate; ++j)
			{
				current_new_offset += string_size;
				res_offsets.push_back(current_new_offset);

				res_chars.resize(res_chars.size() + string_size);
				memcpy(&res_chars[res_chars.size() - string_size], &chars[prev_string_offset], string_size);
			}

			prev_replicate_offset = replicate_offsets[i];
			prev_string_offset = offsets[i];
		}

		return res;
	}

	void reserve(size_t n)
	{
		offsets.reserve(n);
		chars.reserve(n * DBMS_APPROX_STRING_SIZE);
	}

	void getExtremes(Field & min, Field & max) const
	{
		min = String();
		max = String();
	}


	Chars_t & getChars() { return chars; }
	const Chars_t & getChars() const { return chars; }

	Offsets_t & getOffsets() { return offsets; }
	const Offsets_t & getOffsets() const { return offsets; }
};


}
