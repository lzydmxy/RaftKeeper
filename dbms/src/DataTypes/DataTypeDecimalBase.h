#pragma once
#include <cmath>

#include <common/arithmeticOverflow.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeWithSimpleSerialization.h>

#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int DECIMAL_OVERFLOW;
}

class Context;
bool decimalCheckComparisonOverflow(const Context & context);
bool decimalCheckArithmeticOverflow(const Context & context);


static constexpr size_t minDecimalPrecision() { return 1; }
template <typename T> static constexpr size_t maxDecimalPrecision() { return 0; }
template <> constexpr size_t maxDecimalPrecision<Decimal32>() { return 9; }
template <> constexpr size_t maxDecimalPrecision<Decimal64>() { return 18; }
template <> constexpr size_t maxDecimalPrecision<Decimal128>() { return 38; }

inline UInt32 leastDecimalPrecisionFor(TypeIndex int_type)
{
    switch (int_type)
    {
        case TypeIndex::Int8: [[fallthrough]];
        case TypeIndex::UInt8:
            return 3;
        case TypeIndex::Int16: [[fallthrough]];
        case TypeIndex::UInt16:
            return 5;
        case TypeIndex::Int32: [[fallthrough]];
        case TypeIndex::UInt32:
            return 10;
        case TypeIndex::Int64:
            return 19;
        case TypeIndex::UInt64:
            return 20;
        default:
            break;
    }
    return 0;
}

/// Base class for decimals, like Decimal(P, S), where P is precision, S is scale.
/// Maximum precisions for underlying types are:
/// Int32    9
/// Int64   18
/// Int128  38
/// Operation between two decimals leads to Decimal(P, S), where
///     P is one of (9, 18, 38); equals to the maximum precision for the biggest underlying type of operands.
///     S is maximum scale of operands. The allowed valuas are [0, precision]
template <typename T>
class DataTypeDecimalBase : public DataTypeWithSimpleSerialization
{
    static_assert(IsDecimalNumber<T>);

public:
    using FieldType = T;
    using ColumnType = ColumnDecimal<T>;

    static constexpr bool is_parametric = true;

    static constexpr size_t maxPrecision() { return maxDecimalPrecision<T>(); }

    DataTypeDecimalBase(UInt32 precision_, UInt32 scale_)
    :   precision(precision_),
        scale(scale_)
    {
        if (unlikely(precision < 1 || precision > maxPrecision()))
            throw Exception("Precision " + std::to_string(precision) + " is out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        if (unlikely(scale < 0 || static_cast<UInt32>(scale) > maxPrecision()))
            throw Exception("Scale " + std::to_string(scale) + " is out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    TypeIndex getTypeId() const override { return TypeId<T>::value; }

    Field getDefault() const override;
    MutableColumnPtr createColumn() const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return true; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool isComparable() const override { return true; }
    bool isValueRepresentedByNumber() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return sizeof(T); }

    bool isSummable() const override { return true; }
    bool canBeUsedInBooleanContext() const override { return true; }
    bool canBeInsideNullable() const override { return true; }

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;

    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    /// Decimal specific

    UInt32 getPrecision() const { return precision; }
    UInt32 getScale() const { return scale; }
    T getScaleMultiplier() const { return getScaleMultiplier(scale); }

    T wholePart(T x) const
    {
        if (scale == 0)
            return x;
        return x / getScaleMultiplier();
    }

    T fractionalPart(T x) const
    {
        if (scale == 0)
            return 0;
        if (x < T(0))
            x *= T(-1);
        return x % getScaleMultiplier();
    }

    T maxWholeValue() const { return getScaleMultiplier(maxPrecision() - scale) - T(1); }

    bool canStoreWhole(T x) const
    {
        T max = maxWholeValue();
        if (x > max || x < -max)
            return false;
        return true;
    }

    /// @returns multiplier for U to become T with correct scale
    template <typename U>
    T scaleFactorFor(const DataTypeDecimalBase<U> & x, bool) const
    {
        if (getScale() < x.getScale())
            throw Exception("Decimal result's scale is less than argiment's one", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        UInt32 scale_delta = getScale() - x.getScale(); /// scale_delta >= 0
        return getScaleMultiplier(scale_delta);
    }

    template <typename U>
    T scaleFactorFor(const DataTypeNumber<U> & , bool is_multiply_or_divisor) const
    {
        if (is_multiply_or_divisor)
            return 1;
        return getScaleMultiplier();
    }

    static T getScaleMultiplier(UInt32 scale);

protected:
    const UInt32 precision;
    const UInt32 scale;
};


// TODO (vnemkov): enable only if both tx and ty are derived from DecimalBase and are essentially same type with different type-params.
template <typename T, typename U, template <typename> typename DecimalType>
typename std::enable_if_t<(sizeof(T) >= sizeof(U)), DecimalType<T>>
decimalResultType(const DecimalType<T> & tx, const DecimalType<U> & ty, bool is_multiply, bool is_divide)
{
    UInt32 scale = (tx.getScale() > ty.getScale() ? tx.getScale() : ty.getScale());
    if (is_multiply)
        scale = tx.getScale() + ty.getScale();
    else if (is_divide)
        scale = tx.getScale();
    return DecimalType<T>(maxDecimalPrecision<T>(), scale);
}

template <typename T, typename U, template <typename> typename DecimalType>
typename std::enable_if_t<(sizeof(T) < sizeof(U)), const DecimalType<U>>
decimalResultType(const DecimalType<T> & tx, const DecimalType<U> & ty, bool is_multiply, bool is_divide)
{
    UInt32 scale = (tx.getScale() > ty.getScale() ? tx.getScale() : ty.getScale());
    if (is_multiply)
        scale = tx.getScale() * ty.getScale();
    else if (is_divide)
        scale = tx.getScale();
    return DecimalType<U>(maxDecimalPrecision<U>(), scale);
}

template <typename T, typename U, template <typename> typename DecimalType>
const DecimalType<T> decimalResultType(const DecimalType<T> & tx, const DataTypeNumber<U> &, bool, bool)
{
    return DecimalType<T>(maxDecimalPrecision<T>(), tx.getScale());
}

template <typename T, typename U, template <typename> typename DecimalType>
const DecimalType<U> decimalResultType(const DataTypeNumber<T> &, const DecimalType<U> & ty, bool, bool)
{
    return DecimalType<U>(maxDecimalPrecision<U>(), ty.getScale());
}


////// TODO (vnemkov): make that work for DecimalBase-derived types
//template <typename T, template <typename> typename DecimalType>
//inline const DecimalType<T> * checkDecimal(const IDataType & data_type)
//{
//    return typeid_cast<const DecimalType<T> *>(&data_type);
//}

//inline UInt32 getDecimalScale(const IDataType & data_type, UInt32 default_value = std::numeric_limits<UInt32>::max())
//{
//    if (auto * decimal_type = checkDecimal<Decimal32>(data_type))
//        return decimal_type->getScale();
//    if (auto * decimal_type = checkDecimal<Decimal64>(data_type))
//        return decimal_type->getScale();
//    if (auto * decimal_type = checkDecimal<Decimal128>(data_type))
//        return decimal_type->getScale();
//    return default_value;
//}

///

template <typename DataType> constexpr bool IsDataTypeDecimal = false;
template <typename DataType> constexpr bool IsDataTypeDecimalOrNumber = IsDataTypeDecimal<DataType> || IsDataTypeNumber<DataType>;

//template <typename FromDataType, typename ToDataType>
//inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>, typename ToDataType::FieldType>
//convertDecimals(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to)
//{
//    using FromFieldType = typename FromDataType::FieldType;
//    using ToFieldType = typename ToDataType::FieldType;
//    using MaxFieldType = std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)), FromFieldType, ToFieldType>;
//    using MaxNativeType = typename MaxFieldType::NativeType;

//    MaxNativeType converted_value;
//    if (scale_to > scale_from)
//    {
//        converted_value = DataTypeDecimal<MaxFieldType>::getScaleMultiplier(scale_to - scale_from);
//        if (common::mulOverflow(static_cast<MaxNativeType>(value), converted_value, converted_value))
//            throw Exception("Decimal convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
//    }
//    else
//        converted_value = value / DataTypeDecimal<MaxFieldType>::getScaleMultiplier(scale_from - scale_to);

//    if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType))
//    {
//        if (converted_value < std::numeric_limits<typename ToFieldType::NativeType>::min() ||
//            converted_value > std::numeric_limits<typename ToFieldType::NativeType>::max())
//            throw Exception("Decimal convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
//    }

//    return converted_value;
//}

//template <typename FromDataType, typename ToDataType>
//inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeNumber<ToDataType>, typename ToDataType::FieldType>
//convertFromDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
//{
//    using FromFieldType = typename FromDataType::FieldType;
//    using ToFieldType = typename ToDataType::FieldType;

//    if constexpr (std::is_floating_point_v<ToFieldType>)
//        return static_cast<ToFieldType>(value) / FromDataType::getScaleMultiplier(scale);
//    else
//    {
//        FromFieldType converted_value = convertDecimals<FromDataType, FromDataType>(value, scale, 0);

//        if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType) || !std::numeric_limits<ToFieldType>::is_signed)
//        {
//            if constexpr (std::numeric_limits<ToFieldType>::is_signed)
//            {
//                if (converted_value < std::numeric_limits<ToFieldType>::min() ||
//                    converted_value > std::numeric_limits<ToFieldType>::max())
//                    throw Exception("Decimal convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
//            }
//            else
//            {
//                using CastIntType = std::conditional_t<std::is_same_v<ToFieldType, UInt64>, Int128, Int64>;

//                if (converted_value < 0 ||
//                    converted_value > static_cast<CastIntType>(std::numeric_limits<ToFieldType>::max()))
//                    throw Exception("Decimal convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
//            }
//        }
//        return converted_value;
//    }
//}

//template <typename FromDataType, typename ToDataType>
//inline std::enable_if_t<IsDataTypeNumber<FromDataType> && IsDataTypeDecimal<ToDataType>, typename ToDataType::FieldType>
//convertToDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
//{
//    using FromFieldType = typename FromDataType::FieldType;
//    using ToNativeType = typename ToDataType::FieldType::NativeType;

//    if constexpr (std::is_floating_point_v<FromFieldType>)
//    {
//        if (!std::isfinite(value))
//            throw Exception("Decimal convert overflow. Cannot convert infinity or NaN to decimal", ErrorCodes::DECIMAL_OVERFLOW);

//        auto out = value * ToDataType::getScaleMultiplier(scale);
//        if constexpr (std::is_same_v<ToNativeType, Int128>)
//        {
//            static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
//            static constexpr __int128 max_int128 = (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
//            if (out <= static_cast<ToNativeType>(min_int128) || out >= static_cast<ToNativeType>(max_int128))
//                throw Exception("Decimal convert overflow. Float is out of Decimal range", ErrorCodes::DECIMAL_OVERFLOW);
//        }
//        else
//        {
//            if (out <= std::numeric_limits<ToNativeType>::min() || out >= std::numeric_limits<ToNativeType>::max())
//                throw Exception("Decimal convert overflow. Float is out of Decimal range", ErrorCodes::DECIMAL_OVERFLOW);
//        }
//        return out;
//    }
//    else
//    {
//        if constexpr (std::is_same_v<FromFieldType, UInt64>)
//            if (value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
//                return convertDecimals<DataTypeDecimal<Decimal128>, ToDataType>(value, 0, scale);
//        return convertDecimals<DataTypeDecimal<Decimal64>, ToDataType>(value, 0, scale);
//    }
//}

template <template <typename> typename DecimalType>
DataTypePtr createDecimal(UInt64 precision_value, UInt64 scale_value)
{
    if (precision_value < minDecimalPrecision() || precision_value > maxDecimalPrecision<Decimal128>())
        throw Exception("Wrong precision", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (static_cast<UInt64>(scale_value) > precision_value)
        throw Exception("Negative scales and scales larger than precision are not supported", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (precision_value <= maxDecimalPrecision<Decimal32>())
        return std::make_shared<DecimalType<Decimal32>>(precision_value, scale_value);
    else if (precision_value <= maxDecimalPrecision<Decimal64>())
        return std::make_shared<DecimalType<Decimal64>>(precision_value, scale_value);
    return std::make_shared<DecimalType<Decimal128>>(precision_value, scale_value);
}

extern template class DataTypeDecimalBase<Decimal32>;
extern template class DataTypeDecimalBase<Decimal64>;
extern template class DataTypeDecimalBase<Decimal128>;
extern template class DataTypeDecimalBase<DateTime64>;


}
