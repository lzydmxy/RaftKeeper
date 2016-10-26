#pragma once

#include <stddef.h>
#include <cstdint>
#include <utility>
#include <atomic>
#include <DB/Core/Types.h>

/** Allows to count number of simultaneously happening processes or current value of some metric.
  *  - for high-level profiling.
  *
  * See also ProfileEvents.h
  * ProfileEvents counts number of happened events - for example, how many times queries was executed.
  * CurrentMetrics counts number of simultaneously happening events - for example, number of currently executing queries, right now,
  *  or just current value of some metric - for example, replica delay in seconds.
  *
  * CurrentMetrics are updated instantly and are correct for any point in time.
  * For periodically (asynchronously) updated metrics, see AsynchronousMetrics.h
  */

namespace CurrentMetrics
{
	/// Metric identifier (index in array).
	using Metric = size_t;
	using Value = DB::Int64;

	/// Get text description of metric by identifier. Returns statically allocated string.
	const char * getDescription(Metric event);

	/// Metric identifier -> current value of metric.
	extern std::atomic<Value> values[];

	/// Get index just after last metric identifier.
	Metric end();

	/// Set value of specified metric.
	inline void set(Metric metric, Value value)
	{
		values[metric] = value;
	}

	/// Add value for specified metric. You must subtract value later; or see class Increment below.
	inline void add(Metric metric, Value value = 1)
	{
		values[metric] += value;
	}

	inline void sub(Metric metric, Value value = 1)
	{
		add(metric, -value);
	}

	/// For lifetime of object, add amout for specified metric. Then subtract.
	class Increment
	{
	private:
		std::atomic<Value> * what;
		Value amount;

		Increment(std::atomic<Value> * what, Value amount)
			: what(what), amount(amount)
		{
			*what += amount;
		}

	public:
		Increment(Metric metric, Value amount = 1)
			: Increment(&values[metric], amount) {}

		~Increment()
		{
			if (what)
				*what -= amount;
		}

		Increment(Increment && old)
		{
			*this = std::move(old);
		}

		Increment & operator= (Increment && old)
		{
			what = old.what;
			amount = old.amount;
			old.what = nullptr;
			return *this;
		}

		void changeTo(Value new_amount)
		{
			*what += new_amount - amount;
			amount = new_amount;
		}

		/// Subtract value before destructor.
		void destroy()
		{
			*what -= amount;
			what = nullptr;
		}
	};
}
