DROP TABLE IF EXISTS nested_test;
CREATE TABLE nested_test (s String, nest Nested(x UInt8, y UInt32)) ENGINE = Memory;
INSERT INTO nested_test VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
SELECT * FROM nested_test
SELECT s, nest.x, nest.y FROM nested_test ARRAY JOIN nest
SELECT s, nest.x, nest.y FROM nested_test ARRAY JOIN nest.x
SELECT s, nest.x, nest.y FROM nested_test ARRAY JOIN nest.x, nest.ySELECT s, n.x, n.y FROM nested_test ARRAY JOIN nest AS nSELECT s, n.x, n.y, nest.x FROM nested_test ARRAY JOIN nest AS nSELECT s, n.x, n.y, nest.x, nest.y FROM nested_test ARRAY JOIN nest AS nSELECT s, n.x, n.y, nest.x, nest.y, num FROM nested_test ARRAY JOIN nest AS n, arrayEnumerate(nest.x) AS num