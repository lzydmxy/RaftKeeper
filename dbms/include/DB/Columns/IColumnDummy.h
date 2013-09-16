#pragma once

#include <DB/Columns/IColumn.h>
#include <DB/Interpreters/Set.h>


namespace DB
{
	
/** Базовый класс для столбцов-констант, содержащих значение, не входящее в Field.
  * Не является полноценым столбцом и используется особым образом.
  */
class IColumnDummy : public IColumn
{
public:
	IColumnDummy(size_t s_) : s(s_) {}
	
	virtual ColumnPtr cloneDummy(size_t s_) const = 0;
	
	ColumnPtr cloneResized(size_t s_) const { return cloneDummy(s_); }
	bool isConst() { return true; }
	size_t size() const { return s; }
	void insertDefault() { ++s; }
	size_t byteSize() const { return 0; }
	int compareAt(size_t n, size_t m, const IColumn & rhs_) const { return 0; }
	
	Field operator[](size_t n) const { throw Exception("Cannot get value from " + getName(), ErrorCodes::NOT_IMPLEMENTED); }
	void get(size_t n, Field & res) const { throw Exception("Cannot get value from " + getName(), ErrorCodes::NOT_IMPLEMENTED); };
	void insert(const Field & x) { throw Exception("Cannot insert element into " + getName(), ErrorCodes::NOT_IMPLEMENTED); }
	StringRef getDataAt(size_t n) const { throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED); }
	void insertData(const char * pos, size_t length) { throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED); }

	void getExtremes(Field & min, Field & max) const
	{
		throw Exception("Method getExtremes is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	ColumnPtr cut(size_t start, size_t length) const
	{
		return cloneDummy(length);
	}
	
	ColumnPtr filter(const Filter & filt) const
	{
		size_t new_size = 0;
		for (Filter::const_iterator it = filt.begin(); it != filt.end(); ++it)
			if (*it)
				++new_size;
			
			return cloneDummy(new_size);
	}
	
	ColumnPtr permute(const Permutation & perm, size_t limit) const
	{
		if (s != perm.size())
			throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
		
		return cloneDummy(limit ? std::min(s, limit) : s);
	}
	
	Permutation getPermutation(bool reverse, size_t limit) const
	{
		Permutation res(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;
		return res;
	}
	
	ColumnPtr replicate(const Offsets_t & offsets) const
	{
		if (s != offsets.size())
			throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
		
		return cloneDummy(offsets.back());
	}
	
private:
	size_t s;
};
	
}
