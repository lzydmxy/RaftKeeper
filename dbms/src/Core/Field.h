#pragma once

#include <vector>
#include <algorithm>
#include <type_traits>
#include <functional>

#include <Common/Exception.h>
#include <Common/UInt128.h>
#include <Core/Types.h>
#include <Core/Defines.h>
#include <Core/UUID.h>
#include <common/DayNum.h>
#include <common/strong_typedef.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int BAD_GET;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

class Field;
using Array = std::vector<Field>;
using TupleBackend = std::vector<Field>;
STRONG_TYPEDEF(TupleBackend, Tuple) /// Array and Tuple are different types with equal representation inside Field.

template <typename T> bool decimalEqual(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T> bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T> bool decimalLessOrEqual(T x, T y, UInt32 x_scale, UInt32 y_scale);

template <typename T>
class DecimalField
{
public:
    DecimalField(T value, UInt32 scale_)
    :   dec(value),
        scale(scale_)
    {}

    operator T() const { return dec; }
    T getValue() const { return dec; }
    T getScaleMultiplier() const;
    UInt32 getScale() const { return scale; }

    template <typename U>
    bool operator < (const DecimalField<U> & r) const
    {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimalLess<MaxType>(dec, r.getValue(), scale, r.getScale());
    }

    template <typename U>
    bool operator <= (const DecimalField<U> & r) const
    {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimalLessOrEqual<MaxType>(dec, r.getValue(), scale, r.getScale());
    }

    template <typename U>
    bool operator == (const DecimalField<U> & r) const
    {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimalEqual<MaxType>(dec, r.getValue(), scale, r.getScale());
    }

    template <typename U> bool operator > (const DecimalField<U> & r) const { return r < *this; }
    template <typename U> bool operator >= (const DecimalField<U> & r) const { return r <= * this; }
    template <typename U> bool operator != (const DecimalField<U> & r) const { return !(*this == r); }

    const DecimalField<T> & operator += (const DecimalField<T> & r)
    {
        if (scale != r.getScale())
            throw Exception("Add different decimal fields", ErrorCodes::LOGICAL_ERROR);
        dec += r.getValue();
        return *this;
    }

    const DecimalField<T> & operator -= (const DecimalField<T> & r)
    {
        if (scale != r.getScale())
            throw Exception("Sub different decimal fields", ErrorCodes::LOGICAL_ERROR);
        dec -= r.getValue();
        return *this;
    }

private:
    T dec;
    UInt32 scale;
};

/** 32 is enough. Round number is used for alignment and for better arithmetic inside std::vector.
  * NOTE: Actually, sizeof(std::string) is 32 when using libc++, so Field is 40 bytes.
  */
#define DBMS_MIN_FIELD_SIZE 32


/** Discriminated union of several types.
  * Made for replacement of `boost::variant`
  *  is not generalized,
  *  but somewhat more efficient, and simpler.
  *
  * Used to represent a single value of one of several types in memory.
  * Warning! Prefer to use chunks of columns instead of single values. See Column.h
  */
class Field
{
public:
    struct Types
    {
        /// Type tag.
        enum Which
        {
            Null    = 0,
            UInt64  = 1,
            Int64   = 2,
            Float64 = 3,
            UInt128 = 4,
            Int128  = 5,

            /// Non-POD types.

            String  = 16,
            Array   = 17,
            Tuple   = 18,
            Decimal32  = 19,
            Decimal64  = 20,
            Decimal128 = 21,
        };

        static const int MIN_NON_POD = 16;

        static const char * toString(Which which)
        {
            switch (which)
            {
                case Null:    return "Null";
                case UInt64:  return "UInt64";
                case UInt128: return "UInt128";
                case Int64:   return "Int64";
                case Int128:  return "Int128";
                case Float64: return "Float64";
                case String:  return "String";
                case Array:   return "Array";
                case Tuple:   return "Tuple";
                case Decimal32:  return "Decimal32";
                case Decimal64:  return "Decimal64";
                case Decimal128: return "Decimal128";
            }

            throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
        }
    };


    /// Returns an identifier for the type or vice versa.
    template <typename T> struct TypeToEnum;
    template <Types::Which which> struct EnumToType;

    static bool IsDecimal(Types::Which which) { return which >= Types::Decimal32 && which <= Types::Decimal128; }

    Field()
        : which(Types::Null)
    {
    }

    /** Despite the presence of a template constructor, this constructor is still needed,
      *  since, in its absence, the compiler will still generate the default constructor.
      */
    Field(const Field & rhs)
    {
        create(rhs);
    }

    Field(Field && rhs)
    {
        create(std::move(rhs));
    }

    template <typename T>
    Field(T && rhs, std::enable_if_t<!std::is_same_v<std::decay_t<T>, Field>, void *> = nullptr);

    /// Create a string inplace.
    Field(const char * data, size_t size)
    {
        create(data, size);
    }

    Field(const unsigned char * data, size_t size)
    {
        create(data, size);
    }

    /// NOTE In case when field already has string type, more direct assign is possible.
    void assignString(const char * data, size_t size)
    {
        destroy();
        create(data, size);
    }

    void assignString(const unsigned char * data, size_t size)
    {
        destroy();
        create(data, size);
    }

    Field & operator= (const Field & rhs)
    {
        if (this != &rhs)
        {
            if (which != rhs.which)
            {
                destroy();
                create(rhs);
            }
            else
                assign(rhs);    /// This assigns string or vector without deallocation of existing buffer.
        }
        return *this;
    }

    Field & operator= (Field && rhs)
    {
        if (this != &rhs)
        {
            if (which != rhs.which)
            {
                destroy();
                create(std::move(rhs));
            }
            else
                assign(std::move(rhs));
        }
        return *this;
    }

    template <typename T>
    std::enable_if_t<!std::is_same_v<std::decay_t<T>, Field>, Field &>
    operator= (T && rhs);

    ~Field()
    {
        destroy();
    }


    Types::Which getType() const { return which; }
    const char * getTypeName() const { return Types::toString(which); }

    bool isNull() const { return which == Types::Null; }


    template <typename T> T & get()
    {
        using TWithoutRef = std::remove_reference_t<T>;
        TWithoutRef * MAY_ALIAS ptr = reinterpret_cast<TWithoutRef*>(&storage);
        return *ptr;
    }

    template <typename T> const T & get() const
    {
        using TWithoutRef = std::remove_reference_t<T>;
        const TWithoutRef * MAY_ALIAS ptr = reinterpret_cast<const TWithoutRef*>(&storage);
        return *ptr;
    }

    template <typename T> bool tryGet(T & result)
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            return false;
        result = get<T>();
        return true;
    }

    template <typename T> bool tryGet(T & result) const
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            return false;
        result = get<T>();
        return true;
    }

    template <typename T> T & safeGet()
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            throw Exception("Bad get: has " + std::string(getTypeName()) + ", requested " + std::string(Types::toString(requested)), ErrorCodes::BAD_GET);
        return get<T>();
    }

    template <typename T> const T & safeGet() const
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            throw Exception("Bad get: has " + std::string(getTypeName()) + ", requested " + std::string(Types::toString(requested)), ErrorCodes::BAD_GET);
        return get<T>();
    }


    bool operator< (const Field & rhs) const
    {
        if (which < rhs.which)
            return true;
        if (which > rhs.which)
            return false;

        switch (which)
        {
            case Types::Null:    return false;
            case Types::UInt64:  return get<UInt64>()  < rhs.get<UInt64>();
            case Types::UInt128: return get<UInt128>() < rhs.get<UInt128>();
            case Types::Int64:   return get<Int64>()   < rhs.get<Int64>();
            case Types::Int128:  return get<Int128>()  < rhs.get<Int128>();
            case Types::Float64: return get<Float64>() < rhs.get<Float64>();
            case Types::String:  return get<String>()  < rhs.get<String>();
            case Types::Array:   return get<Array>()   < rhs.get<Array>();
            case Types::Tuple:   return get<Tuple>()   < rhs.get<Tuple>();
            case Types::Decimal32:  return get<DecimalField<Decimal32>>()  < rhs.get<DecimalField<Decimal32>>();
            case Types::Decimal64:  return get<DecimalField<Decimal64>>()  < rhs.get<DecimalField<Decimal64>>();
            case Types::Decimal128: return get<DecimalField<Decimal128>>() < rhs.get<DecimalField<Decimal128>>();
        }

        throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
    }

    bool operator> (const Field & rhs) const
    {
        return rhs < *this;
    }

    bool operator<= (const Field & rhs) const
    {
        if (which < rhs.which)
            return true;
        if (which > rhs.which)
            return false;

        switch (which)
        {
            case Types::Null:    return true;
            case Types::UInt64:  return get<UInt64>()  <= rhs.get<UInt64>();
            case Types::UInt128: return get<UInt128>() <= rhs.get<UInt128>();
            case Types::Int64:   return get<Int64>()   <= rhs.get<Int64>();
            case Types::Int128:  return get<Int128>()  <= rhs.get<Int128>();
            case Types::Float64: return get<Float64>() <= rhs.get<Float64>();
            case Types::String:  return get<String>()  <= rhs.get<String>();
            case Types::Array:   return get<Array>()   <= rhs.get<Array>();
            case Types::Tuple:   return get<Tuple>()   <= rhs.get<Tuple>();
            case Types::Decimal32:  return get<DecimalField<Decimal32>>()  <= rhs.get<DecimalField<Decimal32>>();
            case Types::Decimal64:  return get<DecimalField<Decimal64>>()  <= rhs.get<DecimalField<Decimal64>>();
            case Types::Decimal128: return get<DecimalField<Decimal128>>() <= rhs.get<DecimalField<Decimal128>>();
        }

        throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
    }

    bool operator>= (const Field & rhs) const
    {
        return rhs <= *this;
    }

    bool operator== (const Field & rhs) const
    {
        if (which != rhs.which)
            return false;

        switch (which)
        {
            case Types::Null:    return true;
            case Types::UInt64:
            case Types::Int64:
            case Types::Float64: return get<UInt64>()  == rhs.get<UInt64>();
            case Types::String:  return get<String>()  == rhs.get<String>();
            case Types::Array:   return get<Array>()   == rhs.get<Array>();
            case Types::Tuple:   return get<Tuple>()   == rhs.get<Tuple>();
            case Types::UInt128: return get<UInt128>() == rhs.get<UInt128>();
            case Types::Int128:  return get<Int128>()  == rhs.get<Int128>();
            case Types::Decimal32:  return get<DecimalField<Decimal32>>()  == rhs.get<DecimalField<Decimal32>>();
            case Types::Decimal64:  return get<DecimalField<Decimal64>>()  == rhs.get<DecimalField<Decimal64>>();
            case Types::Decimal128: return get<DecimalField<Decimal128>>() == rhs.get<DecimalField<Decimal128>>();
        }

        throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
    }

    bool operator!= (const Field & rhs) const
    {
        return !(*this == rhs);
    }

private:
    std::aligned_union_t<DBMS_MIN_FIELD_SIZE - sizeof(Types::Which),
        Null, UInt64, UInt128, Int64, Int128, Float64, String, Array, Tuple,
        DecimalField<Decimal32>, DecimalField<Decimal64>, DecimalField<Decimal128>
        > storage;

    Types::Which which;


    /// Assuming there was no allocated state or it was deallocated (see destroy).
    template <typename T>
    void createConcrete(T && x)
    {
        using JustT = std::decay_t<T>;
        new (&storage) JustT(std::forward<T>(x));
        which = TypeToEnum<JustT>::value;
    }

    /// Assuming same types.
    template <typename T>
    void assignConcrete(T && x)
    {
        using JustT = std::decay_t<T>;
        JustT * MAY_ALIAS ptr = reinterpret_cast<JustT *>(&storage);
        *ptr = std::forward<T>(x);
    }


    template <typename F, typename Field>    /// Field template parameter may be const or non-const Field.
    static void dispatch(F && f, Field & field)
    {
        switch (field.which)
        {
            case Types::Null:    f(field.template get<Null>());    return;

// gcc 7.3.0
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
            case Types::UInt64:  f(field.template get<UInt64>());  return;
            case Types::UInt128: f(field.template get<UInt128>()); return;
            case Types::Int64:   f(field.template get<Int64>());   return;
            case Types::Int128:  f(field.template get<Int128>());  return;
            case Types::Float64: f(field.template get<Float64>()); return;
#if !__clang__
#pragma GCC diagnostic pop
#endif
            case Types::String:  f(field.template get<String>());  return;
            case Types::Array:   f(field.template get<Array>());   return;
            case Types::Tuple:   f(field.template get<Tuple>());   return;
            case Types::Decimal32:  f(field.template get<DecimalField<Decimal32>>()); return;
            case Types::Decimal64:  f(field.template get<DecimalField<Decimal64>>()); return;
            case Types::Decimal128: f(field.template get<DecimalField<Decimal128>>()); return;

            default:
                throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
        }
    }


    void create(const Field & x)
    {
        dispatch([this] (auto & value) { createConcrete(value); }, x);
    }

    void create(Field && x)
    {
        dispatch([this] (auto & value) { createConcrete(std::move(value)); }, x);
    }

    void assign(const Field & x)
    {
        dispatch([this] (auto & value) { assignConcrete(value); }, x);
    }

    void assign(Field && x)
    {
        dispatch([this] (auto & value) { assignConcrete(std::move(value)); }, x);
    }


    void create(const char * data, size_t size)
    {
        new (&storage) String(data, size);
        which = Types::String;
    }

    void create(const unsigned char * data, size_t size)
    {
        create(reinterpret_cast<const char *>(data), size);
    }

    ALWAYS_INLINE void destroy()
    {
        if (which < Types::MIN_NON_POD)
            return;

        switch (which)
        {
            case Types::String:
                destroy<String>();
                break;
            case Types::Array:
                destroy<Array>();
                break;
            case Types::Tuple:
                destroy<Tuple>();
                break;
            default:
                 break;
        }

        which = Types::Null;    /// for exception safety in subsequent calls to destroy and create, when create fails.
    }

    template <typename T>
    void destroy()
    {
        T * MAY_ALIAS ptr = reinterpret_cast<T*>(&storage);
        ptr->~T();
    }
};

#undef DBMS_MIN_FIELD_SIZE


template <> struct Field::TypeToEnum<Null>    { static const Types::Which value = Types::Null; };
template <> struct Field::TypeToEnum<UInt64>  { static const Types::Which value = Types::UInt64; };
template <> struct Field::TypeToEnum<UInt128> { static const Types::Which value = Types::UInt128; };
template <> struct Field::TypeToEnum<Int64>   { static const Types::Which value = Types::Int64; };
template <> struct Field::TypeToEnum<Int128>  { static const Types::Which value = Types::Int128; };
template <> struct Field::TypeToEnum<Float64> { static const Types::Which value = Types::Float64; };
template <> struct Field::TypeToEnum<String>  { static const Types::Which value = Types::String; };
template <> struct Field::TypeToEnum<Array>   { static const Types::Which value = Types::Array; };
template <> struct Field::TypeToEnum<Tuple>   { static const Types::Which value = Types::Tuple; };
template <> struct Field::TypeToEnum<DecimalField<Decimal32>>{ static const Types::Which value = Types::Decimal32; };
template <> struct Field::TypeToEnum<DecimalField<Decimal64>>{ static const Types::Which value = Types::Decimal64; };
template <> struct Field::TypeToEnum<DecimalField<Decimal128>>{ static const Types::Which value = Types::Decimal128; };

template <> struct Field::EnumToType<Field::Types::Null>    { using Type = Null; };
template <> struct Field::EnumToType<Field::Types::UInt64>  { using Type = UInt64; };
template <> struct Field::EnumToType<Field::Types::UInt128> { using Type = UInt128; };
template <> struct Field::EnumToType<Field::Types::Int64>   { using Type = Int64; };
template <> struct Field::EnumToType<Field::Types::Int128>  { using Type = Int128; };
template <> struct Field::EnumToType<Field::Types::Float64> { using Type = Float64; };
template <> struct Field::EnumToType<Field::Types::String>  { using Type = String; };
template <> struct Field::EnumToType<Field::Types::Array>   { using Type = Array; };
template <> struct Field::EnumToType<Field::Types::Tuple>   { using Type = Tuple; };
template <> struct Field::EnumToType<Field::Types::Decimal32> { using Type = DecimalField<Decimal32>; };
template <> struct Field::EnumToType<Field::Types::Decimal64> { using Type = DecimalField<Decimal64>; };
template <> struct Field::EnumToType<Field::Types::Decimal128> { using Type = DecimalField<Decimal128>; };


template <typename T>
T get(const Field & field)
{
    return field.template get<T>();
}

template <typename T>
T get(Field & field)
{
    return field.template get<T>();
}

template <typename T>
T safeGet(const Field & field)
{
    return field.template safeGet<T>();
}

template <typename T>
T safeGet(Field & field)
{
    return field.template safeGet<T>();
}


template <> struct TypeName<Array> { static std::string get() { return "Array"; } };
template <> struct TypeName<Tuple> { static std::string get() { return "Tuple"; } };


template <typename T> struct NearestFieldType;

template <> struct NearestFieldType<UInt8>   { using Type = UInt64; };
template <> struct NearestFieldType<UInt16>  { using Type = UInt64; };
template <> struct NearestFieldType<UInt32>  { using Type = UInt64; };
template <> struct NearestFieldType<UInt64>  { using Type = UInt64; };
#ifdef __APPLE__
template <> struct NearestFieldType<time_t>  { using Type = UInt64; };
template <> struct NearestFieldType<size_t>  { using Type = UInt64; };
#endif
template <> struct NearestFieldType<DayNum>  { using Type = UInt64; };
template <> struct NearestFieldType<UInt128> { using Type = UInt128; };
template <> struct NearestFieldType<UUID>  { using Type = UInt128; };
template <> struct NearestFieldType<Int8>    { using Type = Int64; };
template <> struct NearestFieldType<Int16>   { using Type = Int64; };
template <> struct NearestFieldType<Int32>   { using Type = Int64; };
template <> struct NearestFieldType<Int64>   { using Type = Int64; };
template <> struct NearestFieldType<Int128>  { using Type = Int128; };
template <> struct NearestFieldType<Decimal32>   { using Type = DecimalField<Decimal32>; };
template <> struct NearestFieldType<Decimal64>   { using Type = DecimalField<Decimal64>; };
template <> struct NearestFieldType<Decimal128>  { using Type = DecimalField<Decimal128>; };
template <> struct NearestFieldType<DecimalField<Decimal32>>   { using Type = DecimalField<Decimal32>; };
template <> struct NearestFieldType<DecimalField<Decimal64>>   { using Type = DecimalField<Decimal64>; };
template <> struct NearestFieldType<DecimalField<Decimal128>>  { using Type = DecimalField<Decimal128>; };
template <> struct NearestFieldType<Float32> { using Type = Float64; };
template <> struct NearestFieldType<Float64> { using Type = Float64; };
template <> struct NearestFieldType<const char*>  { using Type = String; };
template <> struct NearestFieldType<String>  { using Type = String; };
template <> struct NearestFieldType<Array>   { using Type = Array; };
template <> struct NearestFieldType<Tuple>   { using Type = Tuple; };
template <> struct NearestFieldType<bool>    { using Type = UInt64; };
template <> struct NearestFieldType<Null>    { using Type = Null; };

template <typename T>
decltype(auto) nearestFieldType(T && x)
{
    using U = typename NearestFieldType<std::decay_t<T>>::Type;
    if constexpr (std::is_same_v<std::decay_t<T>, U>)
        return std::forward<T>(x);
    else
        return U(x);
}

/// This (rather tricky) code is to avoid ambiguity in expressions like
/// Field f = 1;
/// instead of
/// Field f = Int64(1);
/// Things to note:
/// 1. float <--> int needs explicit cast
/// 2. customized types needs explicit cast
template <typename T>
Field::Field(T && rhs, std::enable_if_t<!std::is_same_v<std::decay_t<T>, Field>, void *>)
{
    auto && val = nearestFieldType(std::forward<T>(rhs));
    createConcrete(std::forward<decltype(val)>(val));
}

template <typename T>
std::enable_if_t<!std::is_same_v<std::decay_t<T>, Field>, Field &>
Field::operator= (T && rhs)
{
    auto && val = nearestFieldType(std::forward<T>(rhs));
    using U = decltype(val);
    if (which != TypeToEnum<std::decay_t<U>>::value)
    {
        destroy();
        createConcrete(std::forward<U>(val));
    }
    else
        assignConcrete(std::forward<U>(val));

    return *this;
}


class ReadBuffer;
class WriteBuffer;

/// It is assumed that all elements of the array have the same type.
void readBinary(Array & x, ReadBuffer & buf);

inline void readText(Array &, ReadBuffer &) { throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED); }
inline void readQuoted(Array &, ReadBuffer &) { throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED); }

/// It is assumed that all elements of the array have the same type.
void writeBinary(const Array & x, WriteBuffer & buf);

void writeText(const Array & x, WriteBuffer & buf);

inline void writeQuoted(const Array &, WriteBuffer &) { throw Exception("Cannot write Array quoted.", ErrorCodes::NOT_IMPLEMENTED); }

void readBinary(Tuple & x, ReadBuffer & buf);

inline void readText(Tuple &, ReadBuffer &) { throw Exception("Cannot read Tuple.", ErrorCodes::NOT_IMPLEMENTED); }
inline void readQuoted(Tuple &, ReadBuffer &) { throw Exception("Cannot read Tuple.", ErrorCodes::NOT_IMPLEMENTED); }

void writeBinary(const Tuple & x, WriteBuffer & buf);

void writeText(const Tuple & x, WriteBuffer & buf);

inline void writeQuoted(const Tuple &, WriteBuffer &) { throw Exception("Cannot write Tuple quoted.", ErrorCodes::NOT_IMPLEMENTED); }
}
