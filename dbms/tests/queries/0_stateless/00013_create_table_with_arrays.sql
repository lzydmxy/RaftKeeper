DROP TABLE IF EXISTS arrays_test;
CREATE TABLE arrays_test (s String, arr Array(UInt8)) ENGINE = Memory;
INSERT INTO arrays_test VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
SELECT * FROM arrays_test
SELECT s, arr FROM arrays_test ARRAY JOIN arr
SELECT s, arr, a FROM arrays_test ARRAY JOIN arr AS aSELECT s, arr, a, num FROM arrays_test ARRAY JOIN arr AS a, arrayEnumerate(arr) AS numSELECT s, arr, a, num, arrayEnumerate(arr) FROM arrays_test ARRAY JOIN arr AS a, arrayEnumerate(arr) AS numSELECT s, arr, a, mapped FROM arrays_test ARRAY JOIN arr AS a, arrayMap(x -> x + 1, arr) AS mapped
SELECT s, arr, a, num, mapped FROM arrays_test ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped
SELECT sumArray(arr), sumArrayIf(arr, s LIKE '%l%'), sumArrayIf(arr, s LIKE '%e%') FROM arrays_test

