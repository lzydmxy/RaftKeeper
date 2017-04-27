SELECT k, x, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, x FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT x, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY FULL JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;

SELECT k, x, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, x FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT x, y FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;
SELECT k FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x) ANY RIGHT JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k WHERE k < 10 ORDER BY k;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (k1 UInt32, k2 UInt32, k3 UInt32, val_t1 String) ENGINE=TinyLog;
CREATE TABLE t2 (val_t2 String, k3 UInt32, k2_alias UInt32, k1 UInt32) ENGINE=TinyLog;

INSERT INTO t1 VALUES (1, 2, 3, 'aaa'), (2, 3, 4, 'bbb');
INSERT INTO t2 VALUES ('ccc', 4, 3, 2), ('ddd', 7, 6, 5);

SELECT k1, k2, k3, val_t1, val_t2 FROM (SELECT * FROM t1) ANY FULL JOIN (SELECT * FROM t2) USING (k3, k1, k2 AS k2_alias) ORDER BY k1, k2, k3;

SELECT k1, k2, k3, val_t1, val_t2 FROM (SELECT * FROM t1) ANY RIGHT JOIN (SELECT * FROM t2) USING (k3, k1, k2 AS k2_alias) ORDER BY k1, k2, k3;
