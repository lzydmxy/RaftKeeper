DROP TABLE IF EXISTS test.minmax_idx;
DROP TABLE IF EXISTS test.minmax_idx_r;
DROP TABLE IF EXISTS test.minmax_idx2;
DROP TABLE IF EXISTS test.minmax_idx2_r;

SET allow_experimental_data_skipping_indices = 1;

CREATE TABLE test.minmax_idx
(
    u64 UInt64,
    i32 Int32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/indices_alter1', 'r1')
ORDER BY u64;

CREATE TABLE test.minmax_idx_r
(
    u64 UInt64,
    i32 Int32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/indices_alter1', 'r2')
ORDER BY u64;

INSERT INTO test.minmax_idx VALUES (1, 2);

SYSTEM SYNC REPLICA test.minmax_idx_r;

ALTER TABLE test.minmax_idx ADD INDEX idx1 u64 * i32 TYPE minmax GRANULARITY 10;
ALTER TABLE test.minmax_idx_r ADD INDEX idx2 u64 + i32 TYPE minmax GRANULARITY 10;
ALTER TABLE test.minmax_idx ADD INDEX idx3 u64 - i32 TYPE minmax GRANULARITY 10 AFTER idx1;

SHOW CREATE TABLE test.minmax_idx;
SHOW CREATE TABLE test.minmax_idx_r;

SELECT * FROM test.minmax_idx WHERE u64 * i32 = 2 ORDER BY (u64, i32);
SELECT * FROM test.minmax_idx_r WHERE u64 * i32 = 2 ORDER BY (u64, i32);

INSERT INTO test.minmax_idx VALUES (1, 4);
INSERT INTO test.minmax_idx_r VALUES (3, 2);
INSERT INTO test.minmax_idx VALUES (1, 5);
INSERT INTO test.minmax_idx_r VALUES (65, 75);
INSERT INTO test.minmax_idx VALUES (19, 9);

SYSTEM SYNC REPLICA test.minmax_idx;
SYSTEM SYNC REPLICA test.minmax_idx_r;

SELECT * FROM test.minmax_idx WHERE u64 * i32 > 1 ORDER BY (u64, i32);
SELECT * FROM test.minmax_idx_r WHERE u64 * i32 > 1 ORDER BY (u64, i32);

ALTER TABLE test.minmax_idx DROP INDEX idx1;

SHOW CREATE TABLE test.minmax_idx;
SHOW CREATE TABLE test.minmax_idx_r;

SELECT * FROM test.minmax_idx WHERE u64 * i32 > 1 ORDER BY (u64, i32);
SELECT * FROM test.minmax_idx_r WHERE u64 * i32 > 1 ORDER BY (u64, i32);

ALTER TABLE test.minmax_idx DROP INDEX idx2;
ALTER TABLE test.minmax_idx_r DROP INDEX idx3;

SHOW CREATE TABLE test.minmax_idx;
SHOW CREATE TABLE test.minmax_idx_r;

ALTER TABLE test.minmax_idx ADD INDEX idx1 u64 * i32 TYPE minmax GRANULARITY 10;

SHOW CREATE TABLE test.minmax_idx;
SHOW CREATE TABLE test.minmax_idx_r;

SELECT * FROM test.minmax_idx WHERE u64 * i32 > 1 ORDER BY (u64, i32);
SELECT * FROM test.minmax_idx_r WHERE u64 * i32 > 1 ORDER BY (u64, i32);


CREATE TABLE test.minmax_idx2
(
    u64 UInt64,
    i32 Int32,
    INDEX idx1 u64 + i32 TYPE minmax GRANULARITY 10,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 10
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/indices_alter2', 'r1')
ORDER BY u64;

CREATE TABLE test.minmax_idx2_r
(
    u64 UInt64,
    i32 Int32,
    INDEX idx1 u64 + i32 TYPE minmax GRANULARITY 10,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 10
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/indices_alter2', 'r2')
ORDER BY u64;


SHOW CREATE TABLE test.minmax_idx2;
SHOW CREATE TABLE test.minmax_idx2_r;

INSERT INTO test.minmax_idx2 VALUES (1, 2);
INSERT INTO test.minmax_idx2_r VALUES (1, 3);

SYSTEM SYNC REPLICA test.minmax_idx2;
SYSTEM SYNC REPLICA test.minmax_idx2_r;

SELECT * FROM test.minmax_idx2 WHERE u64 * i32 >= 2 ORDER BY (u64, i32);
SELECT * FROM test.minmax_idx2_r WHERE u64 * i32 >= 2 ORDER BY (u64, i32);

ALTER TABLE test.minmax_idx2_r DROP INDEX idx1, DROP INDEX idx2;

SHOW CREATE TABLE test.minmax_idx2;
SHOW CREATE TABLE test.minmax_idx2_r;

SELECT * FROM test.minmax_idx2 WHERE u64 * i32 >= 2 ORDER BY (u64, i32);
SELECT * FROM test.minmax_idx2_r WHERE u64 * i32 >= 2 ORDER BY (u64, i32);

DROP TABLE test.minmax_idx;
DROP TABLE test.minmax_idx_r;
DROP TABLE test.minmax_idx2;
DROP TABLE test.minmax_idx2_r;