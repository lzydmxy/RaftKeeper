DROP TABLE IF EXISTS t_enum8;
CREATE TABLE t_enum8( x Enum('hello' = 1, 'world' = 2) ) ENGINE = TinyLog;
INSERT INTO t_enum8 Values('hello'),('world'),('hello');
SELECT * FROM t_enum8;
SELECT CAST(x, 'Int8') FROM t_enum8;
DROP TABLE IF EXISTS t_enum16;
CREATE TABLE t_enum16( x Enum('hello' = 1, 'world' = 128) ) ENGINE = TinyLog;
INSERT INTO t_enum16 Values('hello'),('world'),('hello');
SELECT * FROM t_enum16;
SELECT CAST(x, 'Int16') FROM t_enum16;
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 2, \'b\' = 128)'));
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 2, \'b\' = 127)'));

