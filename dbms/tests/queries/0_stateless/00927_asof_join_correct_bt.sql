DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;

CREATE TABLE A(k UInt32, t UInt32, a UInt64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO A(k,t,a) VALUES (1,101,1),(1,102,2),(1,103,3),(1,104,4),(1,105,5);

CREATE TABLE B(k UInt32, t UInt32, b UInt64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B(k,t,b) VALUES (1,102,2), (1,104,4);
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B USING(k,t) ORDER BY (A.k, A.t);
DROP TABLE B;


CREATE TABLE B(t UInt32, k UInt32, b UInt64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B(k,t,b) VALUES (1,102,2), (1,104,4);
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B USING(k,t) ORDER BY (A.k, A.t);
DROP TABLE B;

CREATE TABLE B(k UInt32, b UInt64, t UInt32) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B(k,t,b) VALUES (1,102,2), (1,104,4);
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B USING(k,t) ORDER BY (A.k, A.t);
DROP TABLE B;

DROP TABLE A;
