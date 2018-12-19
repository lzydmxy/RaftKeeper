SELECT number as x, roundDown(x, [0, 1, 2, 3, 4, 5]) FROM system.numbers LIMIT 10;
SELECT toUInt8(number) as x, roundDown(x, [-1.5, e(), pi(), 5.5]) FROM system.numbers LIMIT 10;
SELECT toInt32(number) as x, roundDown(x, [e(), pi(), pi(), e()]) FROM system.numbers LIMIT 10;
SELECT number as x, roundDown(x, [6, 5, 4]) FROM system.numbers LIMIT 10;
SELECT 1 as x, roundDown(x, [6, 5, 4]);

SET send_logs_level = 'none';
SELECT 1 as x, roundDown(x, []); -- { serverError 43 }
SELECT 1 as x, roundDown(x, emptyArrayUInt8()); -- { serverError 44 }

SELECT 1 as x, roundDown(x, [1]);
SELECT 1 as x, roundDown(x, [1.5]);

SELECT number % 10 as x, roundDown(x, (SELECT groupArray(number * 1.25) FROM numbers(100000))) FROM system.numbers LIMIT 10;

SELECT toDecimal64(number, 5) / 100 as x, roundDown(x, [4, 5, 6]) FROM system.numbers LIMIT 10;

