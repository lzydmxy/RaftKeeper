DROP TABLE IF EXISTS testJoinTable;

CREATE TABLE testJoinTable (number UInt64, data String) ENGINE = Join(ANY, INNER, number);

INSERT INTO testJoinTable VALUES (1, '1'), (2, '2'), (3, '3');

SELECT * FROM (SELECT * FROM numbers(10)) INNER JOIN testJoinTable USING number;
SELECT * FROM (SELECT * FROM numbers(10)) INNER JOIN (SELECT * FROM testJoinTable) USING number;
SELECT * FROM testJoinTable;

DROP TABLE testJoinTable;

SELECT '-';

 SET any_join_distinct_right_table_keys = 1;
 
DROP TABLE IF EXISTS master;
DROP TABLE IF EXISTS transaction;

CREATE TABLE master (id Int32, name String) ENGINE = Join (ANY, LEFT, id);
CREATE TABLE transaction (id Int32, value Float64, master_id Int32) ENGINE = MergeTree() ORDER BY id;

INSERT INTO master VALUES (1, 'ONE');
INSERT INTO transaction VALUES (1, 52.5, 1);

SELECT tx.id, tx.value, m.name FROM transaction tx ANY LEFT JOIN master m ON m.id = tx.master_id;

DROP TABLE master;
DROP TABLE transaction;

SELECT '-';

DROP TABLE IF EXISTS some_join;
DROP TABLE IF EXISTS tbl;

CREATE TABLE some_join (id String, value String) ENGINE = Join(ANY, LEFT, id);
CREATE TABLE tbl (eventDate Date, id String) ENGINE = MergeTree() PARTITION BY tuple() ORDER BY eventDate;

SELECT * FROM tbl AS t ANY LEFT JOIN some_join USING (id);
SELECT * FROM tbl AS t ANY LEFT JOIN some_join AS d USING (id);

DROP TABLE some_join;
DROP TABLE tbl;
