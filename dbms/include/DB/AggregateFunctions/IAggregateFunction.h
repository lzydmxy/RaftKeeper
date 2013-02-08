#pragma once

#include <memory>

#include <Poco/SharedPtr.h>

#include <DB/Core/Row.h>
#include <DB/DataTypes/IDataType.h>


namespace DB
{


typedef char * AggregateDataPtr;
typedef const char * ConstAggregateDataPtr;


/** Интерфейс для агрегатных функций.
  * Экземпляры классов с этим интерфейсом не содержат самих данных для агрегации,
  *  а содержат лишь метаданные (описание) агрегатной функции,
  *  а также методы для создания, удаления и работы с данными.
  * Данные, получающиеся во время агрегации (промежуточные состояния вычислений), хранятся в других объектах
  *  (которые могут быть созданы в каком-нибудь пуле),
  *  а IAggregateFunction - внешний интерфейс для манипулирования ими.
  */
class IAggregateFunction
{
public:
	/// Получить основное имя функции.
	virtual String getName() const = 0;

	/// Получить строку, по которой можно потом будет создать объект того же типа (с помощью AggregateFunctionFactory)
	virtual String getTypeID() const = 0;

	/** Указать типы аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	  * Необходимо вызывать перед остальными вызовами.
	  */
	virtual void setArguments(const DataTypes & arguments) = 0;

	/** Указать параметры - для параметрических агрегатных функций.
	  * Если параметры не предусмотрены или переданные параметры недопустимы - кинуть исключение.
	  * Если параметры есть - необходимо вызывать перед остальными вызовами, иначе - не вызывать.
	  */
	virtual void setParameters(const Row & params)
	{
		throw Exception("Aggregate function " + getName() + " doesn't allow parameters.", ErrorCodes::AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS);
	}

	/// Получить тип результата.
	virtual DataTypePtr getReturnType() const = 0;

	virtual ~IAggregateFunction() {};


	/** Функции по работе с данными. */

	/** Создать пустые данные для агрегации с помощью placement new в заданном месте.
	  * Вы должны будете уничтожить их с помощью метода destroyData.
	  */
	virtual void create(AggregateDataPtr place) const = 0;

	/// Уничтожить данные для агрегации.
	virtual void destroy(AggregateDataPtr place) const = 0;

	/// Получить sizeof структуры с данными.
	virtual void sizeOfData() const = 0;

	/// Как должна быть выровнена структура с данными.
	virtual void alignOfData() const = 0;

	/// Добавить значение.
	virtual void add(AggregateDataPtr place, const Row & row) const = 0;

	/// Объединить состояние с другим состоянием.
	/// Нельзя объединять с "пустым" состоянием, то есть - состоянием, для которого ни разу не был выполнен метод add.
	virtual void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const = 0;

	/// Сериализовать состояние (например, для передачи по сети). Нельзя сериализовывать "пустое" состояние.
	virtual void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const = 0;

	/// Десериализовать состояние и объединить своё состояние с ним.
	virtual void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const = 0;

	/// Получить результат
	virtual Field getResult(ConstAggregateDataPtr place) const = 0;
};


/// Реализует несколько методов. T - тип структуры с данными для агрегации.
template <typename T>
class IAggregateFunctionHelper : public IAggregateFunction
{
protected:
	typedef T Data;
	
	static Data & data(AggregateDataPtr place) 				{ return *reinterpret_cast<Data*>(place); }
	static const Data & data(ConstAggregateDataPtr place)	{ return *reinterpret_cast<const Data*>(place); }
	
public:
	/** Создать пустые данные для агрегации с помощью placement new в заданном месте.
	  * Вы должны будете уничтожить их с помощью метода destroyData.
	  */
	void create(AggregateDataPtr place) const
	{
		new (place) Data;
	}

	/// Уничтожить данные для агрегации.
	void destroy(AggregateDataPtr place) const
	{
		data(place).~Data();
	}

	/// Получить sizeof структуры с данными.
	void sizeOfData() const
	{
		return sizeof(Data);
	}

	/// Как должна быть выровнена структура с данными.
	void alignOfData() const
	{
		return __alignof__(Data);
	}
};


using Poco::SharedPtr;

typedef SharedPtr<IAggregateFunction> AggregateFunctionPtr;

}
