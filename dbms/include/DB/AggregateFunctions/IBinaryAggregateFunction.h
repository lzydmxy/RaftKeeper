#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>

namespace DB
{

template <typename T, typename Derived>
class IBinaryAggregateFunction : public IAggregateFunctionHelper<T>
{
private:
	Derived & getDerived() { return static_cast<Derived &>(*this); }
	const Derived & getDerived() const { return static_cast<const Derived &>(*this); }

public:
	void setArguments(const DataTypes & arguments) override final
	{
		if (arguments.size() != 2)
			throw Exception{
				"Passed " + toString(arguments.size()) + " arguments to binary aggregate function " + this->getName(),
					ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		getDerived().setArgumentsImpl(arguments);
	}

	void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num) const override final
	{
		getDerived().addImpl(place, *columns[0], *columns[1], row_num);
	}

	static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num)
	{
		return static_cast<const Derived &>(*that).addImpl(place, *columns[0], *columns[1], row_num);
	}

	IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }
};

}
