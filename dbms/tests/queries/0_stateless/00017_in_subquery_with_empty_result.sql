SELECT count() FROM (SELECT * FROM system.numbers LIMIT 1000) WHERE 1 IN (SELECT 0 WHERE 0)
FORMAT JSON
