#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{


/** Не агрегатная функция, а адаптер агрегатных функций,
  *  который любую агрегатную функцию agg(x) делает агрегатной функцией вида aggIf(x, cond).
  * Адаптированная агрегатная функция принимает два аргумента - значение и условие,
  *  и вычисляет вложенную агрегатную функцию для значений при выполненном условии.
  * Например, avgIf(x, cond) вычисляет среднее x при условии cond.
  */
class AggregateFunctionIf : public IAggregateFunction
{
private:
	AggregateFunctionPtr nested_func_owner;
	IAggregateFunction * nested_func;
public:
	AggregateFunctionIf(AggregateFunctionPtr nested_) : nested_func_owner(nested_), nested_func(nested_func_owner.get()) {}
	
	String getName() const
	{
		return nested_func->getName() + "If";
	}
	
	DataTypePtr getReturnType() const
	{
		return nested_func->getReturnType();
	}

	void setArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 2)
			throw Exception("Aggregate function " + getName() + " requires exactly two arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		if (!dynamic_cast<const DataTypeUInt8 *>(&*arguments[1]))
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument for aggregate function " + getName() + ". Must be UInt8.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		DataTypes nested_arguments;
		nested_arguments.push_back(arguments[0]);
		nested_func->setArguments(nested_arguments);
	}

	void setParameters(const Row & params)
	{
		nested_func->setParameters(params);
	}

	void create(AggregateDataPtr place) const
	{
		nested_func->create(place);
	}

	void destroy(AggregateDataPtr place) const
	{
		nested_func->destroy(place);
	}

	size_t sizeOfData() const
	{
		return nested_func->sizeOfData();
	}

	size_t alignOfData() const
	{
		return nested_func->alignOfData();
	}

	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const
	{
		if (static_cast<const ColumnUInt8 &>(*columns[1]).getData()[row_num])
			nested_func->add(place, columns, row_num);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		nested_func->merge(place, rhs);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		nested_func->serialize(place, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		nested_func->deserializeMerge(place, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		nested_func->insertResultInto(place, to);
	}
};

}
