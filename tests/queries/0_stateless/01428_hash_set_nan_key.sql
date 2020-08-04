SELECT uniqExact(nan) FROM numbers(1000);
SELECT uniqExact(number % inf) FROM numbers(1000);
SELECT sumDistinct(number % inf) FROM numbers(1000);
SELECT DISTINCT number % inf FROM numbers(1000);

SELECT topKWeightedMerge(1)(initializeAggregation('topKWeightedState(1)', nan, arrayJoin(range(10))));

select number % inf k from numbers(256) group by k;

SELECT uniqExact(reinterpretAsFloat64(reinterpretAsFixedString(reinterpretAsUInt64(reinterpretAsFixedString(nan)) + number))) FROM numbers(10);
