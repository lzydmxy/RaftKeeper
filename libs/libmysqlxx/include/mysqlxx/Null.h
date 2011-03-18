#ifndef MYSQLXX_NULL_H
#define MYSQLXX_NULL_H

#include <mysqlxx/Exception.h>


namespace mysqlxx
{


struct NullType {};
const NullType null = {};


/** Класс для NULL-able типов.
  * Использование:
  *		mysqlxx::Null<int> x = mysqlxx::null;
  *		std::cout << (x.isNull() ? "Ok." : "Fail.") << std::endl;
  *		x = 10;
  */
template <typename T>
class Null
{
public:
	T data;
	bool is_null;
	
	Null() : is_null(true) {}
	Null(NullType data) : is_null(true) {}
	explicit Null(const T & data_) : data(data_), is_null(false) {}

	operator T & ()
	{
		if (is_null)
			throw Exception("Value is NULL");
		return data;
	}

	operator const T & () const
	{
		if (is_null)
			throw Exception("Value is NULL");
		return data;
	}

	Null<T> & operator= (const T & data_) { is_null = false; data = data_; return *this; }
	Null<T> & operator= (const Null<T> & other) { is_null = other.is_null; data = other.data; return *this; }
	Null<T> & operator= (const NullType other) { is_null = true; data = T(); return *this; }

	bool isNull() const { return is_null; }

	bool operator< (const Null<T> & other) const
	{
		return is_null < other.is_null
			|| (is_null == other.is_null && data < other.data);
	}

	bool operator< (const NullType other) const { return false; }

	bool operator== (const Null<T> & other) const
	{
		return is_null == other.is_null && data == other.data;
	}

	bool operator== (const T & other) const
	{
		return !is_null && data == other;
	}

	bool operator== (const NullType other) const { return is_null; }

	bool operator!= (const Null<T> & other) const 
	{
		return !(*this == other);
	}

	bool operator!= (const NullType other) const { return !is_null; }

	bool operator!= (const T & other) const
	{
		return is_null || data != other;
	}
};


}

#endif
