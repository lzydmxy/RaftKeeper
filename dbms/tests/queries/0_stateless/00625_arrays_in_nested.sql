USE test;

DROP TABLE IF EXISTS nested;
CREATE TABLE nested
(
    column Nested
    (
        name String,
        names Array(String),
        types Array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3))
    )
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO nested VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);

SELECT * FROM nested;

DETACH TABLE nested;
ATTACH TABLE nested;

SELECT * FROM nested;


DROP TABLE IF EXISTS nested;
CREATE TABLE nested
(
    column Nested
    (
        name String,
        names Array(String),
        types Array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3))
    )
) ENGINE = Log;

INSERT INTO nested VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);

SELECT * FROM nested;


DROP TABLE IF EXISTS nested;
CREATE TABLE nested
(
    column Nested
    (
        name String,
        names Array(String),
        types Array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3))
    )
) ENGINE = TinyLog;

INSERT INTO nested VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);

SELECT * FROM nested;


DROP TABLE IF EXISTS nested;
CREATE TABLE nested
(
    column Nested
    (
        name String,
        names Array(String),
        types Array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3))
    )
) ENGINE = StripeLog;

INSERT INTO nested VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);

SELECT * FROM nested;


DROP TABLE IF EXISTS nested;
CREATE TABLE nested
(
    column Nested
    (
        name String,
        names Array(String),
        types Array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3))
    )
) ENGINE = Memory;

INSERT INTO nested VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);

SELECT * FROM nested;


DROP TABLE nested;
