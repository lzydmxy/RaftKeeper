#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Common/NaNUtils.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataStreams/ColumnGathererStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT;
}


ColumnNullable::ColumnNullable(ColumnPtr nested_column_, ColumnPtr null_map_)
    : nested_column{nested_column_}, null_map{null_map_}
{
    if (nested_column->isNullable())
        throw Exception{"A nullable column cannot contain another nullable column", ErrorCodes::ILLEGAL_COLUMN};

    if (nested_column->isConst())
        throw Exception{"A nullable column cannot contain constant nested column", ErrorCodes::ILLEGAL_COLUMN};

    /// TODO Also check for Nullable(Array(...)). But they are occasionally used somewhere in tests.

    if (typeid_cast<const ColumnTuple *>(nested_column.get()))
        throw Exception{"Nullable(Tuple(...)) is illegal", ErrorCodes::ILLEGAL_COLUMN};

    if (typeid_cast<const ColumnAggregateFunction *>(nested_column.get()))
        throw Exception{"Nullable(AggregateFunction(...)) is illegal", ErrorCodes::ILLEGAL_COLUMN};

    /// ColumnNullable cannot have constant nested column. But constant argument could be passed. Materialize it.
    if (auto nested_column_materialized = nested_column->convertToFullColumnIfConst())
        nested_column = nested_column_materialized;

    if (null_map->isConst())
        throw Exception{"ColumnNullable cannot have constant null map", ErrorCodes::ILLEGAL_COLUMN};
}


size_t ColumnNullable::sizeOfField() const
{
    if (nested_column->isFixed())
        return getNullMapConcreteColumn().sizeOfField() + nested_column->sizeOfField();

    throw Exception("Cannot get sizeOfField() for column " + getName(), ErrorCodes::CANNOT_GET_SIZE_OF_FIELD);
}


ColumnPtr ColumnNullable::convertToFullColumnIfConst() const
{
    ColumnPtr new_col_holder;

    if (auto full_col = nested_column->convertToFullColumnIfConst())
        new_col_holder = std::make_shared<ColumnNullable>(full_col, null_map);

    return new_col_holder;
}


void ColumnNullable::updateHashWithValue(size_t n, SipHash & hash) const
{
    const auto & arr = getNullMap();
    hash.update(reinterpret_cast<const char *>(&arr[n]), sizeof(arr[0]));
    if (arr[n] == 0)
        nested_column->updateHashWithValue(n, hash);
}


ColumnPtr ColumnNullable::cloneResized(size_t new_size) const
{
    ColumnPtr new_nested_col = nested_column->cloneResized(new_size);
    auto new_null_map = std::make_shared<ColumnUInt8>();

    if (new_size > 0)
    {
        new_null_map->getData().resize(new_size);

        size_t count = std::min(size(), new_size);
        memcpy(new_null_map->getData().data(), getNullMap().data(), count * sizeof(getNullMap()[0]));

        /// If resizing to bigger one, set all new values to NULLs.
        if (new_size > count)
            memset(&new_null_map->getData()[count], 1, new_size - count);
    }

    return std::make_shared<ColumnNullable>(new_nested_col, new_null_map);
}


Field ColumnNullable::operator[](size_t n) const
{
    if (isNullAt(n))
        return Null();
    else
    {
        const IColumn & col = *nested_column;
        return col[n];
    }
}


void ColumnNullable::get(size_t n, Field & res) const
{
    if (isNullAt(n))
        res = Null();
    else
        nested_column->get(n, res);
}

StringRef ColumnNullable::getDataAt(size_t /*n*/) const
{
    throw Exception{"Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED};
}

void ColumnNullable::insertData(const char * /*pos*/, size_t /*length*/)
{
    throw Exception{"Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED};
}

StringRef ColumnNullable::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    const auto & arr = getNullMap();
    static constexpr auto s = sizeof(arr[0]);

    auto pos = arena.allocContinue(s, begin);
    memcpy(pos, &arr[n], s);

    size_t nested_size = 0;

    if (arr[n] == 0)
        nested_size = nested_column->serializeValueIntoArena(n, arena, begin).size;

    return StringRef{begin, s + nested_size};
}

const char * ColumnNullable::deserializeAndInsertFromArena(const char * pos)
{
    UInt8 val = *reinterpret_cast<const UInt8 *>(pos);
    pos += sizeof(val);

    getNullMap().push_back(val);

    if (val == 0)
        pos = nested_column->deserializeAndInsertFromArena(pos);
    else
        nested_column->insertDefault();

    return pos;
}

void ColumnNullable::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(src);
    getNullMapConcreteColumn().insertRangeFrom(*nullable_col.null_map, start, length);
    nested_column->insertRangeFrom(*nullable_col.nested_column, start, length);
}

void ColumnNullable::insert(const Field & x)
{
    if (x.isNull())
    {
        nested_column->insertDefault();
        getNullMap().push_back(1);
    }
    else
    {
        nested_column->insert(x);
        getNullMap().push_back(0);
    }
}

void ColumnNullable::insertFrom(const IColumn & src, size_t n)
{
    const ColumnNullable & src_concrete = static_cast<const ColumnNullable &>(src);
    nested_column->insertFrom(*src_concrete.getNestedColumn(), n);
    getNullMap().push_back(src_concrete.getNullMap()[n]);
}

void ColumnNullable::popBack(size_t n)
{
    nested_column->popBack(n);
    getNullMapConcreteColumn().popBack(n);
}

ColumnPtr ColumnNullable::filter(const Filter & filt, ssize_t result_size_hint) const
{
    ColumnPtr filtered_data = nested_column->filter(filt, result_size_hint);
    ColumnPtr filtered_null_map = getNullMapConcreteColumn().filter(filt, result_size_hint);
    return std::make_shared<ColumnNullable>(filtered_data, filtered_null_map);
}

ColumnPtr ColumnNullable::permute(const Permutation & perm, size_t limit) const
{
    ColumnPtr permuted_data = nested_column->permute(perm, limit);
    ColumnPtr permuted_null_map = getNullMapConcreteColumn().permute(perm, limit);
    return std::make_shared<ColumnNullable>(permuted_data, permuted_null_map);
}

int ColumnNullable::compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const
{
    /// NULL values share the properties of NaN values.
    /// Here the last parameter of compareAt is called null_direction_hint
    /// instead of the usual nan_direction_hint and is used to implement
    /// the ordering specified by either NULLS FIRST or NULLS LAST in the
    /// ORDER BY construction.

    const ColumnNullable & nullable_rhs = static_cast<const ColumnNullable &>(rhs_);

    bool lval_is_null = isNullAt(n);
    bool rval_is_null = nullable_rhs.isNullAt(m);

    if (unlikely(lval_is_null || rval_is_null))
    {
        if (lval_is_null && rval_is_null)
            return 0;
        else
            return lval_is_null ? null_direction_hint : -null_direction_hint;
    }

    const IColumn & nested_rhs = *(nullable_rhs.getNestedColumn());
    return nested_column->compareAt(n, m, nested_rhs, null_direction_hint);
}

void ColumnNullable::getPermutation(bool reverse, size_t limit, int null_direction_hint, Permutation & res) const
{
    /// Cannot pass limit because of unknown amount of NULLs.
    nested_column->getPermutation(reverse, 0, null_direction_hint, res);

    if ((null_direction_hint > 0) != reverse)
    {
        /// Shift all NULL values to the end.

        size_t read_idx = 0;
        size_t write_idx = 0;
        size_t end_idx = res.size();

        if (!limit)
            limit = end_idx;
        else
            limit = std::min(end_idx, limit);

        while (read_idx < limit && !isNullAt(res[read_idx]))
        {
            ++read_idx;
            ++write_idx;
        }

        ++read_idx;

        /// Invariants:
        ///  write_idx < read_idx
        ///  write_idx points to NULL
        ///  read_idx will be incremented to position of next not-NULL
        ///  there are range of NULLs between write_idx and read_idx - 1,
        /// We are moving elements from end to begin of this range,
        ///  so range will "bubble" towards the end.
        /// Relative order of NULL elements could be changed,
        ///  but relative order of non-NULLs is preserved.

        while (read_idx < end_idx && write_idx < limit)
        {
            if (!isNullAt(res[read_idx]))
            {
                std::swap(res[read_idx], res[write_idx]);
                ++write_idx;
            }
            ++read_idx;
        }
    }
    else
    {
        /// Shift all NULL values to the beginning.

        ssize_t read_idx = res.size() - 1;
        ssize_t write_idx = res.size() - 1;

        while (read_idx >= 0 && !isNullAt(res[read_idx]))
        {
            --read_idx;
            --write_idx;
        }

        --read_idx;

        while (read_idx >= 0 && write_idx >= 0)
        {
            if (!isNullAt(res[read_idx]))
            {
                std::swap(res[read_idx], res[write_idx]);
                --write_idx;
            }
            --read_idx;
        }
    }
}

void ColumnNullable::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnNullable::reserve(size_t n)
{
    nested_column->reserve(n);
    getNullMap().reserve(n);
}

size_t ColumnNullable::byteSize() const
{
    return nested_column->byteSize() + getNullMapConcreteColumn().byteSize();
}

size_t ColumnNullable::allocatedBytes() const
{
    return nested_column->allocatedBytes() + getNullMapConcreteColumn().allocatedBytes();
}


namespace
{

/// The following function implements a slightly more general version
/// of getExtremes() than the implementation from ColumnVector.
/// It takes into account the possible presence of nullable values.
template <typename T>
void getExtremesFromNullableContent(const ColumnVector<T> & col, const NullMap & null_map, Field & min, Field & max)
{
    const auto & data = col.getData();
    size_t size = data.size();

    if (size == 0)
    {
        min = Null();
        max = Null();
        return;
    }

    bool has_not_null = false;
    bool has_not_nan = false;

    T cur_min = 0;
    T cur_max = 0;

    for (size_t i = 0; i < size; ++i)
    {
        const T x = data[i];

        if (null_map[i])
            continue;

        if (!has_not_null)
        {
            cur_min = x;
            cur_max = x;
            has_not_null = true;
            has_not_nan = !isNaN(x);
            continue;
        }

        if (isNaN(x))
            continue;

        if (!has_not_nan)
        {
            cur_min = x;
            cur_max = x;
            has_not_nan = true;
            continue;
        }

        if (x < cur_min)
            cur_min = x;
        else if (x > cur_max)
            cur_max = x;
    }

    if (has_not_null)
    {
        min = typename NearestFieldType<T>::Type(cur_min);
        max = typename NearestFieldType<T>::Type(cur_max);
    }
}

}


void ColumnNullable::getExtremes(Field & min, Field & max) const
{
    min = Null();
    max = Null();

    const auto & null_map = getNullMap();

    if (const auto col = typeid_cast<const ColumnInt8 *>(nested_column.get()))
        getExtremesFromNullableContent<Int8>(*col, null_map, min, max);
    else if (const auto col = typeid_cast<const ColumnInt16 *>(nested_column.get()))
        getExtremesFromNullableContent<Int16>(*col, null_map, min, max);
    else if (const auto col = typeid_cast<const ColumnInt32 *>(nested_column.get()))
        getExtremesFromNullableContent<Int32>(*col, null_map, min, max);
    else if (const auto col = typeid_cast<const ColumnInt64 *>(nested_column.get()))
        getExtremesFromNullableContent<Int64>(*col, null_map, min, max);
    else if (const auto col = typeid_cast<const ColumnUInt8 *>(nested_column.get()))
        getExtremesFromNullableContent<UInt8>(*col, null_map, min, max);
    else if (const auto col = typeid_cast<const ColumnUInt16 *>(nested_column.get()))
        getExtremesFromNullableContent<UInt16>(*col, null_map, min, max);
    else if (const auto col = typeid_cast<const ColumnUInt32 *>(nested_column.get()))
        getExtremesFromNullableContent<UInt32>(*col, null_map, min, max);
    else if (const auto col = typeid_cast<const ColumnUInt64 *>(nested_column.get()))
        getExtremesFromNullableContent<UInt64>(*col, null_map, min, max);
    else if (const auto col = typeid_cast<const ColumnFloat32 *>(nested_column.get()))
        getExtremesFromNullableContent<Float32>(*col, null_map, min, max);
    else if (const auto col = typeid_cast<const ColumnFloat64 *>(nested_column.get()))
        getExtremesFromNullableContent<Float64>(*col, null_map, min, max);
}


ColumnPtr ColumnNullable::replicate(const Offsets_t & offsets) const
{
    ColumnPtr replicated_data = nested_column->replicate(offsets);
    ColumnPtr replicated_null_map = getNullMapConcreteColumn().replicate(offsets);
    return std::make_shared<ColumnNullable>(replicated_data, replicated_null_map);
}


template <bool negative>
void ColumnNullable::applyNullMapImpl(const ColumnUInt8 & map)
{
    NullMap & arr1 = getNullMap();
    const NullMap & arr2 = map.getData();

    if (arr1.size() != arr2.size())
        throw Exception{"Inconsistent sizes of ColumnNullable objects", ErrorCodes::LOGICAL_ERROR};

    for (size_t i = 0, size = arr1.size(); i < size; ++i)
        arr1[i] |= negative ^ arr2[i];
}


void ColumnNullable::applyNullMap(const ColumnUInt8 & map)
{
    applyNullMapImpl<false>(map);
}

void ColumnNullable::applyNegatedNullMap(const ColumnUInt8 & map)
{
    applyNullMapImpl<true>(map);
}


void ColumnNullable::applyNullMap(const ColumnNullable & other)
{
    applyNullMap(other.getNullMapConcreteColumn());
}


void ColumnNullable::checkConsistency() const
{
    if (null_map->size() != nested_column->size())
        throw Exception("Logical error: Sizes of nested column and null map of Nullable column are not equal",
            ErrorCodes::SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT);
}

}
