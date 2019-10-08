CREATE TABLE `таблица_со_странным_названием` (a UInt64, b UInt64) ENGINE = Log;
INSERT INTO `таблица_со_странным_названием` VALUES (1, 1);
SELECT * FROM `таблица_со_странным_названием`;
DROP TABLE `таблица_со_странным_названием`;

CREATE DATABASE testlazy ENGINE = Lazy(1);
CREATE TABLE testlazy.`таблица_со_странным_названием` (a UInt64, b UInt64) ENGINE = Log;
INSERT INTO testlazy.`таблица_со_странным_названием` VALUES (1, 1);
SELECT * FROM testlazy.`таблица_со_странным_названием`;
DROP TABLE testlazy.`таблица_со_странным_названием`;
DROP DATABASE testlazy
