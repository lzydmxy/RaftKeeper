SELECT toDate('2015-02-05') IN ('2015-02-04', '2015-02-05');
SELECT toDate('2015-02-05') IN ('2015-02-04', '2015-02-06');
SELECT toDateTime('2015-02-03 04:05:06') IN ('2015-02-03 04:05:06', '2015-02-03 05:06:07');
SELECT toDateTime('2015-02-03 04:05:06') IN ('2015-02-04 04:05:06', '2015-02-03 05:06:07');
SELECT toDate('2015-02-05') NOT IN ('2015-02-04', '2015-02-05');
SELECT toDate('2015-02-05') NOT IN ('2015-02-04', '2015-02-06');
SELECT toDateTime('2015-02-03 04:05:06') NOT IN ('2015-02-03 04:05:06', '2015-02-03 05:06:07');
SELECT toDateTime('2015-02-03 04:05:06') NOT IN ('2015-02-04 04:05:06', '2015-02-03 05:06:07');
