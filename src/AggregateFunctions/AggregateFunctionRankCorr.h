#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitors.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <limits>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>


#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <type_traits>

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB
{

template <template <typename> class Comparator>
struct ComparePairFirst final
{
    template <typename X, typename Y>
    bool operator()(const std::pair<X, Y> & lhs, const std::pair<X, Y> & rhs) const
    {
        return Comparator<X>{}(lhs.first, rhs.first);
    }
};


template <template <typename> class Comparator>
struct ComparePairSecond final
{
    template <typename X, typename Y>
    bool operator()(const std::pair<X, Y> & lhs, const std::pair<X, Y> & rhs) const
    {
        return Comparator<Y>{}(lhs.second, rhs.second);
    }
};

template <typename X = Float64, typename Y = Float64>
struct AggregateFunctionRankCorrData final
{
    size_t size_x = 0;

    using Allocator = MixedAlignedArenaAllocator<alignof(std::pair<X, Y>), 4096>;
    using Array = PODArray<std::pair<X, Y>, 32, Allocator>;

    Array values;
};

template <typename X = Float64, typename Y = Float64>
class AggregateFunctionRankCorr : public
                                         IAggregateFunctionDataHelper<
                                             AggregateFunctionRankCorrData<X, Y>,
                                             AggregateFunctionRankCorr<X, Y>
                                         >
{
    using Data = AggregateFunctionRankCorrData<X, Y>;
    using Allocator = MixedAlignedArenaAllocator<alignof(std::pair<Float64, Float64>), 4096>;
    using Array = PODArray<std::pair<Float64, Float64>, 32, Allocator>;

public:
    AggregateFunctionRankCorr(
        const DataTypes & arguments
    ):
        IAggregateFunctionDataHelper<
            AggregateFunctionRankCorrData<X, Y>,
            AggregateFunctionRankCorr<X, Y>
        > ({arguments}, {})
    {
        // notice: arguments has been in factory
    }

    String getName() const override
    {
        return "RankCorr";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<Int8>>();
    }

    void insert(Data & a, const std::pair<X, Y> & x, Arena * arena) const
    {
        ++a.size_x;
        a.values.push_back(x, arena);
    }

    void add(
        AggregateDataPtr place,
        const IColumn ** columns,
        size_t row_num,
        Arena * arena
    ) const override
    {
        auto & a = this->data(place);

        auto new_x = assert_cast<const ColumnVector<X> &>(*columns[0]).getData()[row_num];
        auto new_y = assert_cast<const ColumnVector<Y> &>(*columns[1]).getData()[row_num];

        auto new_arg = std::make_pair(new_x, new_y);

        a.size_x += 1;

        a.values.push_back(new_arg, arena);
    }

    void merge(
        AggregateDataPtr place,
        ConstAggregateDataPtr rhs,
        Arena * arena
    ) const override
    {
        auto & a = this->data(place);
        auto & b = this->data(rhs);

        if (b.size_x)
        {
            for (size_t i = 0; i < b.size_x; ++i)
                insert(a, b.values[i], arena);
        }
    }

    void serialize(
        ConstAggregateDataPtr place,
        WriteBuffer & buf
    ) const override
    {
        const auto & value = this->data(place).values;
        size_t size = this->data(place).size_x;
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(value.data()), size * sizeof(value[0]));
    }

    void deserialize(
        AggregateDataPtr place,
        ReadBuffer & buf,
        Arena * arena
    ) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        auto & value = this->data(place).values;

        value.resize(size, arena);
        buf.read(reinterpret_cast<char *>(value.data()), size * sizeof(value[0]));
    }

    void insertResultInto(
        AggregateDataPtr place,
        IColumn & to
    ) const override
    {
        const auto & value = this->data(place).values;
        size_t size = this->data(place).size_x;

        if (size < 2)
        {
            throw Exception("Aggregate function " + getName() + " requires samples to be of size > 1", ErrorCodes::BAD_ARGUMENTS);
        }

        //create a copy of values not to format data
        PODArrayWithStackMemory<std::pair<Float64, Float64>, 32> tmp_values;
        tmp_values.resize(size);
        for (size_t j = 0; j < size; ++ j){
            tmp_values[j] = static_cast<std::pair<Float64, Float64>>(value[j]);
        }

//        size_t k = 1;
//        if (k){
//            throw Exception("passed making tmp array", ErrorCodes::BAD_ARGUMENTS);
//        }

        //sort x_values
        std::sort(std::begin(tmp_values), std::end(tmp_values), ComparePairFirst<std::greater>{});

        for (size_t j = 0; j < size; ){
            //replace x_values with their ranks
            size_t same = 1;
            size_t cur_sum = 0;
            size_t rank = j + 1;
            size_t cur_start = j;

            while (j < size - 1)
            {
                if (value[j].first == value[j + 1].first)
                {
                    // rank of (j + 1)th number
                    rank += 1;
                    same++;
                    cur_sum += rank;
                    j++;
                }
                else
                {
                    break;
                }
            }

            // insert rank is calculated as average of ranks of equal values
            Float64 insert_rank = static_cast<Float64>(cur_sum) / same;
            for (size_t i = cur_start; i <= j; ++i) {
                tmp_values[i].first = insert_rank;
            }
            j++;
        }

        //sort y_values
        std::sort(std::begin(tmp_values), std::end(tmp_values), ComparePairSecond<std::greater>{});

        //replace y_values with their ranks
        for (size_t j = 0; j < size; ){
            //replace x_values with their ranks
            size_t same = 1;
            size_t cur_sum = 0;
            size_t rank = j + 1;
            size_t cur_start = j;

            while (j < size - 1)
            {
                if (value[j].second == value[j + 1].second)
                {
                    // rank of (j + 1)th number
                    rank += 1;
                    same++;
                    cur_sum += rank;
                    j++;
                }
                else
                {
                    break;
                }
            }

            // insert rank is calculated as average of ranks of equal values
            Float64 insert_rank = static_cast<Float64>(cur_sum) / same;
            for (size_t i = cur_start; i <= j; ++i) {
                tmp_values[i].second = insert_rank;
            }
            j++;
        }

        //count d^2 sum
        Float64 answ = static_cast<Float64>(0);
        for (size_t j = 0; j < size; ++ j)
        {
            answ += (tmp_values[j].first - tmp_values[j].second) * (tmp_values[j].first - tmp_values[j].second);
        }

        answ *= 6;
        answ /= size * (size * size - 1);

        answ = 1 - answ;

        auto & column = static_cast<ColumnVector<Int8> &>(to);
        if (answ > 0)
        {
            column.getData().push_back(static_cast<Int8>(1));
        }
        else if (answ < 0)
        {
            column.getData().push_back(static_cast<Int8>(-1));
        }
        else
        {
            column.getData().push_back(static_cast<Int8>(0));
        }
    }

};

};
