-- { echo }
SELECT gcd(9223372036854775807, -9223372036854775808); -- { serverError 407 }
SELECT gcd(9223372036854775808, -9223372036854775807); -- { serverError 407 }
SELECT gcd(-9223372036854775808, 9223372036854775807); -- { serverError 407 }
SELECT gcd(-9223372036854775807, 9223372036854775808); -- { serverError 407 }
SELECT gcd(9223372036854775808, -1); -- { serverError 407 }
SELECT lcm(-170141183460469231731687303715884105728, -170141183460469231731687303715884105728); -- { serverError 43 }
SELECT lcm(toInt128(-170141183460469231731687303715884105728), toInt128(-170141183460469231731687303715884105728)); -- { serverError 407 }
SELECT lcm(toInt128(-170141183460469231731687303715884105720), toInt128(-170141183460469231731687303715884105720)); -- { serverError 407 }
SELECT lcm(toInt128('-170141183460469231731687303715884105720'), toInt128('-170141183460469231731687303715884105720'));
SELECT gcd(-9223372036854775806, -9223372036854775806);
