CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.check_query_comment_column;

CREATE TABLE test.check_query_comment_column
  (
    first_column UInt8 DEFAULT 1 COMMENT 'first comment',
    second_column UInt8 MATERIALIZED first_column COMMENT 'second comment',
    third_column UInt8 ALIAS second_column COMMENT 'third comment',
    fourth_column UInt8 COMMENT 'fourth comment',
    fifth_column UInt8
  ) ENGINE = TinyLog;

SHOW CREATE TABLE test.check_query_comment_column;

SELECT table, name, comment
FROM system.columns
WHERE table = 'check_query_comment_column' AND database = 'test'
FORMAT PrettyCompactNoEscapes;

ALTER TABLE test.check_query_comment_column
  COMMENT COLUMN first_column 'another first column',
  COMMENT COLUMN second_column 'another second column',
  COMMENT COLUMN third_column 'another third column',
  COMMENT COLUMN fourth_column 'another fourth column',
  COMMENT COLUMN fifth_column 'another fifth column';

SHOW CREATE TABLE test.check_query_comment_column;

SELECT table, name, comment
FROM system.columns
WHERE table = 'check_query_comment_column' AND database = 'test'
FORMAT PrettyCompactNoEscapes;

DROP TABLE IF EXISTS test.check_query_comment_column;


CREATE TABLE test.check_query_comment_column
  (
    first_column Date COMMENT 'first comment',
    second_column UInt8 COMMENT 'second comment',
    third_column UInt8 COMMENT 'third comment'
  ) ENGINE = MergeTree(first_column, (second_column, second_column), 8192);

SHOW CREATE TABLE test.check_query_comment_column;

SELECT table, name, comment
FROM system.columns
WHERE table = 'check_query_comment_column' AND database = 'test'
FORMAT PrettyCompactNoEscapes;

ALTER TABLE test.check_query_comment_column
  COMMENT COLUMN first_column 'another first comment',
  COMMENT COLUMN second_column 'another second comment',
  COMMENT COLUMN third_column 'another third comment';

SHOW CREATE TABLE test.check_query_comment_column;

SELECT table, name, comment
FROM system.columns
WHERE table = 'check_query_comment_column' and database = 'test'
FORMAT PrettyCompactNoEscapes;

DROP TABLE IF test.check_query_comment_column;

CREATE TABLE test.check_query_comment_column
  (
    first_column UInt8 COMMENT 'first comment'
  ) ENGINE = TinyLog;

ALTER TABLE test.check_query_comment_column MODIFY COLUMN first_column COMMENT 'another comment';

SELECT table, name, comment
FROM system.columns
WHERE table = 'check_query_comment_column' and database = 'test'
FORMAT PrettyCompactNoEscapes;

DROP TABLE IF EXISTS test.check_query_comment_column;
