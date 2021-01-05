SELECT ALL 'a';
SELECT DISTINCT 'a';
SELECT ALL * FROM (SELECT 1 UNION ALL SELECT 1);
SELECT DISTINCT * FROM (SELECT 2 UNION ALL SELECT 2);

SELECT sum(number) FROM numbers(10);
SELECT sum(ALL number) FROM numbers(10);
SELECT sum(DISTINCT number) FROM numbers(10);

SELECT sum(ALL x) FROM (SELECT 1 x UNION ALL SELECT 1);
SELECT sum(DISTINCT x) FROM (SELECT 1 x UNION ALL SELECT 1);
