SET enable_debug_queries = 1;
SET optimize_monotonous_functions_in_order_by = 1;

SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number));
SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number));
SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number));
SELECT number FROM numbers(3) ORDER BY -number;
SELECT number FROM numbers(3) ORDER BY exp(number);
SELECT roundToExp2(number) AS x FROM numbers(3) ORDER BY x, toFloat32(x);
SELECT number AS x FROM numbers(3) ORDER BY toFloat32(x) as k, toFloat64(k);
SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number)) DESC;
SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number)) DESC;
SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number)) DESC;
SELECT number FROM numbers(3) ORDER BY -number DESC;
SELECT number FROM numbers(3) ORDER BY exp(number) DESC;
SELECT roundToExp2(number) AS x FROM numbers(3) ORDER BY x DESC, toFloat32(x) DESC;
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number));
analyze SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number));
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number));
analyze SELECT number FROM numbers(3) ORDER BY -number;
analyze SELECT number FROM numbers(3) ORDER BY exp(number);
analyze SELECT roundToExp2(number) AS x FROM numbers(3) ORDER BY x, toFloat32(x);
analyze SELECT number AS x FROM numbers(3) ORDER BY toFloat32(x) as k, toFloat64(k);
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number)) DESC;
analyze SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number)) DESC;
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number)) DESC;
analyze SELECT number FROM numbers(3) ORDER BY -number DESC;
analyze SELECT number FROM numbers(3) ORDER BY exp(number) DESC;
analyze SELECT roundToExp2(number) AS x FROM numbers(3) ORDER BY x DESC, toFloat32(x) DESC;

SET optimize_monotonous_functions_in_order_by = 0;

SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number));
SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number));
SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number));
SELECT number FROM numbers(3) ORDER BY -number;
SELECT number FROM numbers(3) ORDER BY exp(number);
SELECT roundToExp2(number) AS x FROM numbers(3) ORDER BY x, toFloat32(x);
SELECT number AS x FROM numbers(3) ORDER BY toFloat32(x) as k, toFloat64(k);
SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number)) DESC;
SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number)) DESC;
SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number)) DESC;
SELECT number FROM numbers(3) ORDER BY -number DESC;
SELECT number FROM numbers(3) ORDER BY exp(number) DESC;
SELECT roundToExp2(number) AS x FROM numbers(3) ORDER BY x DESC, toFloat32(x) DESC;
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number));
analyze SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number));
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number));
analyze SELECT number FROM numbers(3) ORDER BY -number;
analyze SELECT number FROM numbers(3) ORDER BY exp(number);
analyze SELECT roundToExp2(number) AS x FROM numbers(3) ORDER BY x, toFloat32(x);
analyze SELECT number AS x FROM numbers(3) ORDER BY toFloat32(x) as k, toFloat64(k);
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number)) DESC;
analyze SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number)) DESC;
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number)) DESC;
analyze SELECT number FROM numbers(3) ORDER BY -number DESC;
analyze SELECT number FROM numbers(3) ORDER BY exp(number) DESC;
analyze SELECT roundToExp2(number) AS x FROM numbers(3) ORDER BY x DESC, toFloat32(x) DESC;
-- TODO: exp() should be monotonous function
