SET allow_experimental_decimal_type = 1;
CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.decimal;

CREATE TABLE IF NOT EXISTS test.decimal
(
    d1 DECIMAL(9, 8),
    d2 DECIMAL(18, 8),
    d3 DECIMAL(38, 8)
)
ENGINE = MergeTree
PARTITION BY toInt32(d1)
ORDER BY (d2, d3);

INSERT INTO test.decimal (d1, d2, d3)
    SELECT toDecimal32(number % 10, 8), toDecimal64(number, 8), toDecimal128(number, 8) FROM system.numbers LIMIT 50;

SELECT count() FROM test.decimal WHERE d1 = 1;
--SELECT * FROM test.decimal WHERE d1 <= toDecimal32(2, 8) ORDER BY d2 DESC;

DROP TABLE test.decimal;
