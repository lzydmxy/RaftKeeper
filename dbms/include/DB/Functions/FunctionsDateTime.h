#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>

#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции работы с датой и временем.
  *
  * toYear, toMonth, toDayOfMonth, toDayOfWeek, toHour, toMinute, toSecond,
  * toMonday, toStartOfMonth, toStartOfYear, toStartOfMinute, toStartOfHour
  * toTime,
  * now
  * TODO: makeDate, makeDateTime
  * 
  * (toDate - расположена в файле FunctionsConversion.h)
  *
  * Возвращаемые типы:
  *  toYear -> UInt16
  *  toMonth, toDayOfMonth, toDayOfWeek, toHour, toMinute, toSecond -> UInt8
  *  toMonday, toStartOfMonth, toStartOfYear -> Date
  *  toStartOfMinute, toStartOfHour, toTime, now -> DateTime
  *
  * А также:
  *
  * timeSlot(EventTime)
  * - округляет время до получаса.
  * 
  * timeSlots(StartTime, Duration)
  * - для интервала времени, начинающегося в StartTime и продолжающегося Duration секунд,
  *   возвращает массив моментов времени, состоящий из округлений вниз до получаса точек из этого интервала.
  *  Например, timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')].
  *  Это нужно для поиска хитов, входящих в соответствующий визит.
  */


#define TIME_SLOT_SIZE 1800


struct ToYearImpl
{
	static inline UInt16 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toYear(t); }
	static inline UInt16 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toYear(DayNum_t(d)); }
};

struct ToMonthImpl
{
	static inline UInt8 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toMonth(t); }
	static inline UInt8 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toMonth(DayNum_t(d)); }
};

struct ToDayOfMonthImpl
{
	static inline UInt8 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toDayOfMonth(t); }
	static inline UInt8 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toDayOfMonth(DayNum_t(d)); }
};

struct ToDayOfWeekImpl
{
	static inline UInt8 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toDayOfWeek(t); }
	static inline UInt8 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toDayOfWeek(DayNum_t(d)); }
};

struct ToHourImpl
{
	static inline UInt8 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toHourInaccurate(t); }
	static inline UInt8 execute(UInt16 d, DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToMinuteImpl
{
	static inline UInt8 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toMinuteInaccurate(t); }
	static inline UInt8 execute(UInt16 d, DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToSecondImpl
{
	static inline UInt8 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toSecondInaccurate(t); }
	static inline UInt8 execute(UInt16 d, DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toSecond", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToMondayImpl
{
	static inline UInt16 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toFirstDayNumOfWeek(date_lut.toDayNum(t)); }
	static inline UInt16 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toFirstDayNumOfWeek(DayNum_t(d)); }
};

struct ToStartOfMonthImpl
{
	static inline UInt16 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toFirstDayNumOfMonth(date_lut.toDayNum(t)); }
	static inline UInt16 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toFirstDayNumOfMonth(DayNum_t(d)); }
};

struct ToStartOfQuarterImpl
{
	static inline UInt16 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toFirstDayNumOfQuarter(date_lut.toDayNum(t)); }
	static inline UInt16 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toFirstDayNumOfQuarter(DayNum_t(d)); }
};

struct ToStartOfYearImpl
{
	static inline UInt16 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toFirstDayNumOfYear(date_lut.toDayNum(t)); }
	static inline UInt16 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toFirstDayNumOfYear(DayNum_t(d)); }
};


struct ToTimeImpl
{
	/// При переводе во время, дату будем приравнивать к 1970-01-02.
	static inline UInt32 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toTimeInaccurate(t) + 86400; }
	static inline UInt32 execute(UInt16 d, DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToStartOfMinuteImpl
{
	static inline UInt32 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toStartOfMinuteInaccurate(t); }
	static inline UInt32 execute(UInt16 d, DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toStartOfMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToStartOfHourImpl
{
	static inline UInt32 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toStartOfHourInaccurate(t); }
	static inline UInt32 execute(UInt16 d, DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toStartOfHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToRelativeYearNumImpl
{
	static inline UInt16 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toYear(t); }
	static inline UInt16 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toYear(DayNum_t(d)); }
};

struct ToRelativeMonthNumImpl
{
	static inline UInt16 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toRelativeMonthNum(t); }
	static inline UInt16 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toRelativeMonthNum(DayNum_t(d)); }
};

struct ToRelativeWeekNumImpl
{
	static inline UInt16 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toRelativeWeekNum(t); }
	static inline UInt16 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toRelativeWeekNum(DayNum_t(d)); }
};

struct ToRelativeDayNumImpl
{
	static inline UInt16 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toRelativeDayNum(t); }
	static inline UInt16 execute(UInt16 d, DateLUTSingleton & date_lut) { return date_lut.toRelativeDayNum(DayNum_t(d)); }
};


struct ToRelativeHourNumImpl
{
	static inline UInt32 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toRelativeHourNum(t); }
	static inline UInt32 execute(UInt16 d, DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toStartOfHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToRelativeMinuteNumImpl
{
	static inline UInt32 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toRelativeMinuteNum(t); }
	static inline UInt32 execute(UInt16 d, DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toStartOfHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};

struct ToRelativeSecondNumImpl
{
	static inline UInt32 execute(UInt32 t, DateLUTSingleton & date_lut) { return date_lut.toRelativeSecondNum(t); }
	static inline UInt32 execute(UInt16 d, DateLUTSingleton & date_lut)
	{
		throw Exception("Illegal type Date of argument for function toStartOfHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};


template <typename FromType, typename ToType, typename Transform, typename Name>
struct DateTimeTransformImpl
{
	static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		DateLUTSingleton & date_lut = DateLUTSingleton::instance();

		if (const ColumnVector<FromType> * col_from = dynamic_cast<const ColumnVector<FromType> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToType> * col_to = new ColumnVector<ToType>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<FromType>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<ToType>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::execute(vec_from[i], date_lut);
		}
		else if (const ColumnConst<FromType> * col_from = dynamic_cast<const ColumnConst<FromType> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<ToType>(col_from->size(), Transform::execute(col_from->getData(), date_lut));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::get(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <typename ToDataType, typename Transform, typename Name>
class FunctionDateOrDateTimeToSomething : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new ToDataType;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		IDataType * from_type = &*block.getByPosition(arguments[0]).type;
		
		if (dynamic_cast<const DataTypeDate *>(from_type))
			DateTimeTransformImpl<DataTypeDate::FieldType, typename ToDataType::FieldType, Transform, Name>::execute(block, arguments, result);
		else if (dynamic_cast<const DataTypeDateTime * >(from_type))
			DateTimeTransformImpl<DataTypeDateTime::FieldType, typename ToDataType::FieldType, Transform, Name>::execute(block, arguments, result);
		else
			throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};


/// Получить текущее время. (Оно - константа, вычисляется один раз за весь запрос.)
class FunctionNow : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "now";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 0)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new DataTypeDateTime;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		block.getByPosition(result).column = new ColumnConstUInt32(
			block.rowsInFirstColumn(),
			time(0));
	}
};


class FunctionTimeSlot : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "timeSlot";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!dynamic_cast<const DataTypeDateTime *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeDateTime;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnUInt32 * times = dynamic_cast<const ColumnUInt32 *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnUInt32 * res = new ColumnUInt32;
			ColumnUInt32::Container_t & res_vec = res->getData();
			const ColumnUInt32::Container_t & vec = times->getData();
						
			size_t size = vec.size();
			res_vec.resize(size);

			for (size_t i = 0; i < size; ++i)
				res_vec[i] = vec[i] / TIME_SLOT_SIZE * TIME_SLOT_SIZE;

			block.getByPosition(result).column = res;
		}
		else if (const ColumnConstUInt32 * const_times = dynamic_cast<const ColumnConstUInt32 *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConstUInt32(block.rowsInFirstColumn(), const_times->getData() / TIME_SLOT_SIZE * TIME_SLOT_SIZE);
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <typename DurationType>
struct TimeSlotsImpl
{
	static void vector_vector(
		const PODArray<UInt32> & starts, const PODArray<DurationType> & durations,
		PODArray<UInt32> & result_values, ColumnArray::Offsets_t & result_offsets)
	{
		size_t size = starts.size();

		result_offsets.resize(size);
		result_values.reserve(size);
		
		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			for (UInt32 value = starts[i] / TIME_SLOT_SIZE; value <= (starts[i] + durations[i]) / TIME_SLOT_SIZE; ++value)
			{
				result_values.push_back(value * TIME_SLOT_SIZE);
				++current_offset;
			}

			result_offsets[i] = current_offset;
		}
	}

	static void vector_constant(
		const PODArray<UInt32> & starts, DurationType duration,
		PODArray<UInt32> & result_values, ColumnArray::Offsets_t & result_offsets)
	{
		size_t size = starts.size();

		result_offsets.resize(size);
		result_values.reserve(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			for (UInt32 value = starts[i] / TIME_SLOT_SIZE; value <= (starts[i] + duration) / TIME_SLOT_SIZE; ++value)
			{
				result_values.push_back(value * TIME_SLOT_SIZE);
				++current_offset;
			}

			result_offsets[i] = current_offset;
		}
	}

	static void constant_vector(
		UInt32 start, const PODArray<DurationType> & durations,
		PODArray<UInt32> & result_values, ColumnArray::Offsets_t & result_offsets)
	{
		size_t size = durations.size();

		result_offsets.resize(size);
		result_values.reserve(size);

		ColumnArray::Offset_t current_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			for (UInt32 value = start / TIME_SLOT_SIZE; value <= (start + durations[i]) / TIME_SLOT_SIZE; ++value)
			{
				result_values.push_back(value * TIME_SLOT_SIZE);
				++current_offset;
			}

			result_offsets[i] = current_offset;
		}
	}

	static void constant_constant(
		UInt32 start, DurationType duration,
		Array & result)
	{
		for (UInt32 value = start / TIME_SLOT_SIZE; value <= (start + duration) / TIME_SLOT_SIZE; ++value)
			result.push_back(static_cast<UInt64>(value * TIME_SLOT_SIZE));
	}
};


class FunctionTimeSlots : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "timeSlots";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!dynamic_cast<const DataTypeDateTime *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!dynamic_cast<const DataTypeUInt32 *>(&*arguments[1]))
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be UInt32.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeArray(new DataTypeDateTime);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnUInt32 * starts = dynamic_cast<const ColumnUInt32 *>(&*block.getByPosition(arguments[0]).column);
		const ColumnConstUInt32 * const_starts = dynamic_cast<const ColumnConstUInt32 *>(&*block.getByPosition(arguments[0]).column);

		const ColumnUInt32 * durations = dynamic_cast<const ColumnUInt32 *>(&*block.getByPosition(arguments[1]).column);
		const ColumnConstUInt32 * const_durations = dynamic_cast<const ColumnConstUInt32 *>(&*block.getByPosition(arguments[1]).column);

		ColumnArray * res = new ColumnArray(new ColumnUInt32);
		ColumnUInt32::Container_t & res_values = dynamic_cast<ColumnUInt32 &>(res->getData()).getData();

		if (starts && durations)
		{
			TimeSlotsImpl<UInt32>::vector_vector(starts->getData(), durations->getData(), res_values, res->getOffsets());
			block.getByPosition(result).column = res;
		}
		else if (starts && const_durations)
		{
			TimeSlotsImpl<UInt32>::vector_constant(starts->getData(), const_durations->getData(), res_values, res->getOffsets());
			block.getByPosition(result).column = res;
		}
		else if (const_starts && durations)
		{
			TimeSlotsImpl<UInt32>::constant_vector(const_starts->getData(), durations->getData(), res_values, res->getOffsets());
			block.getByPosition(result).column = res;
		}
		else if (const_starts && const_durations)
		{
			Array const_res;
			TimeSlotsImpl<UInt32>::constant_constant(const_starts->getData(), const_durations->getData(), const_res);
			block.getByPosition(result).column = new ColumnConstArray(block.rowsInFirstColumn(), const_res, new DataTypeArray(new DataTypeDateTime));
		}
		else
			throw Exception("Illegal columns " + block.getByPosition(arguments[0]).column->getName()
					+ ", " + block.getByPosition(arguments[1]).column->getName()
					+ " of arguments of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NameToYear 			{ static const char * get() { return "toYear"; } };
struct NameToMonth 			{ static const char * get() { return "toMonth"; } };
struct NameToDayOfMonth		{ static const char * get() { return "toDayOfMonth"; } };
struct NameToDayOfWeek		{ static const char * get() { return "toDayOfWeek"; } };
struct NameToHour 			{ static const char * get() { return "toHour"; } };
struct NameToMinute			{ static const char * get() { return "toMinute"; } };
struct NameToSecond			{ static const char * get() { return "toSecond"; } };
struct NameToMonday			{ static const char * get() { return "toMonday"; } };
struct NameToStartOfMonth	{ static const char * get() { return "toStartOfMonth"; } };
struct NameToStartOfQuarter	{ static const char * get() { return "toStartOfQuarter"; } };
struct NameToStartOfYear	{ static const char * get() { return "toStartOfYear"; } };
struct NameToStartOfMinute	{ static const char * get() { return "toStartOfMinute"; } };
struct NameToStartOfHour	{ static const char * get() { return "toStartOfHour"; } };
struct NameToTime 			{ static const char * get() { return "toTime"; } };
struct NameToRelativeYearNum	{ static const char * get() { return "toRelativeYearNum"; } };
struct NameToRelativeMonthNum	{ static const char * get() { return "toRelativeMonthNum"; } };
struct NameToRelativeWeekNum	{ static const char * get() { return "toRelativeWeekNum"; } };
struct NameToRelativeDayNum		{ static const char * get() { return "toRelativeDayNum"; } };
struct NameToRelativeHourNum	{ static const char * get() { return "toRelativeHourNum"; } };
struct NameToRelativeMinuteNum	{ static const char * get() { return "toRelativeMinuteNum"; } };
struct NameToRelativeSecondNum	{ static const char * get() { return "toRelativeSecondNum"; } };


typedef FunctionDateOrDateTimeToSomething<DataTypeUInt16,	ToYearImpl, 		NameToYear> 		FunctionToYear;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToMonthImpl, 		NameToMonth> 		FunctionToMonth;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToDayOfMonthImpl, 	NameToDayOfMonth> 	FunctionToDayOfMonth;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToDayOfWeekImpl, 	NameToDayOfWeek> 	FunctionToDayOfWeek;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToHourImpl, 		NameToHour> 		FunctionToHour;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToMinuteImpl, 		NameToMinute> 		FunctionToMinute;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt8,	ToSecondImpl, 		NameToSecond> 		FunctionToSecond;
typedef FunctionDateOrDateTimeToSomething<DataTypeDate,		ToMondayImpl, 		NameToMonday> 		FunctionToMonday;
typedef FunctionDateOrDateTimeToSomething<DataTypeDate,		ToStartOfMonthImpl, NameToStartOfMonth> FunctionToStartOfMonth;
typedef FunctionDateOrDateTimeToSomething<DataTypeDate,	ToStartOfQuarterImpl, 	NameToStartOfQuarter> 	FunctionToStartOfQuarter;
typedef FunctionDateOrDateTimeToSomething<DataTypeDate,		ToStartOfYearImpl, 	NameToStartOfYear> 	FunctionToStartOfYear;
typedef FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToStartOfMinuteImpl, NameToStartOfMinute> FunctionToStartOfMinute;
typedef FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToStartOfHourImpl, 	NameToStartOfHour> 	FunctionToStartOfHour;
typedef FunctionDateOrDateTimeToSomething<DataTypeDateTime,	ToTimeImpl, 		NameToTime> 		FunctionToTime;

typedef FunctionDateOrDateTimeToSomething<DataTypeUInt16,	ToRelativeYearNumImpl, 		NameToRelativeYearNum> 		FunctionToRelativeYearNum;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeMonthNumImpl, 	NameToRelativeMonthNum> 	FunctionToRelativeMonthNum;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeWeekNumImpl, 		NameToRelativeWeekNum> 		FunctionToRelativeWeekNum;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeDayNumImpl, 		NameToRelativeDayNum> 		FunctionToRelativeDayNum;

typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeHourNumImpl, 		NameToRelativeHourNum> 		FunctionToRelativeHourNum;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeMinuteNumImpl, 	NameToRelativeMinuteNum> 	FunctionToRelativeMinuteNum;
typedef FunctionDateOrDateTimeToSomething<DataTypeUInt32,	ToRelativeSecondNumImpl, 	NameToRelativeSecondNum> 	FunctionToRelativeSecondNum;


}
