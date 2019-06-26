DROP TABLE IF EXISTS alter_00061;
CREATE TABLE alter_00061 (d Date, k UInt64, i32 Int32) ENGINE=MergeTree(d, k, 8192);

INSERT INTO alter_00061 VALUES ('2015-01-01', 10, 42);

DESC TABLE alter_00061;
SHOW CREATE TABLE alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER TABLE alter_00061 ADD COLUMN n Nested(ui8 UInt8, s String);
INSERT INTO alter_00061 VALUES ('2015-01-01', 8, 40, [1,2,3], ['12','13','14']);

DESC TABLE alter_00061;
SHOW CREATE TABLE alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER TABLE alter_00061 ADD COLUMN `n.d` Array(Date);
INSERT INTO alter_00061 VALUES ('2015-01-01', 7, 39, [10,20,30], ['120','130','140'],['2000-01-01','2000-01-01','2000-01-03']);

DESC TABLE alter_00061;
SHOW CREATE TABLE alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER TABLE alter_00061 ADD COLUMN s String DEFAULT '0';
INSERT INTO alter_00061 VALUES ('2015-01-01', 6,38,[10,20,30],['asd','qwe','qwe'],['2000-01-01','2000-01-01','2000-01-03'],'100500');

DESC TABLE alter_00061;
SHOW CREATE TABLE alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER TABLE alter_00061 DROP COLUMN `n.d`, MODIFY COLUMN s Int64;

DESC TABLE alter_00061;
SHOW CREATE TABLE alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER TABLE alter_00061 ADD COLUMN `n.d` Array(Date), MODIFY COLUMN s UInt32;

DESC TABLE alter_00061;
SHOW CREATE TABLE alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

OPTIMIZE TABLE alter_00061;

SELECT * FROM alter_00061 ORDER BY k;

ALTER TABLE alter_00061 DROP COLUMN n.ui8, DROP COLUMN n.d;

DESC TABLE alter_00061;
SHOW CREATE TABLE alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER TABLE alter_00061 DROP COLUMN n.s;

DESC TABLE alter_00061;
SHOW CREATE TABLE alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER TABLE alter_00061 ADD COLUMN n.s Array(String), ADD COLUMN n.d Array(Date);

DESC TABLE alter_00061;
SHOW CREATE TABLE alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

ALTER TABLE alter_00061 DROP COLUMN n;

DESC TABLE alter_00061;
SHOW CREATE TABLE alter_00061;
SELECT * FROM alter_00061 ORDER BY k;

DROP TABLE alter_00061;
