SELECT sum(cityHash64(*)) FROM (SELECT CounterID, quantileTiming(0.5)(SendTiming), count() FROM remote('127.0.0.{1,2,3,4,5,6,7,8,9,10}', test.hits) WHERE SendTiming != -1 GROUP BY CounterID);
