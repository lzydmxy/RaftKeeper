DROP TEMPORARY TABLE IF EXISTS temp_tab;
CREATE TEMPORARY TABLE temp_tab (number UInt64);
INSERT INTO temp_tab SELECT number FROM system.numbers LIMIT 1;
SELECT number FROM temp_tab;
SET send_logs_level = 'fatal';
EXISTS TEMPORARY TABLE temp_tab;
DROP TABLE temp_tab;
EXISTS TEMPORARY TABLE temp_tab;
SET send_logs_level = 'warning';
CREATE TEMPORARY TABLE temp_tab (number UInt64);
SELECT number FROM temp_tab;
DROP TEMPORARY TABLE temp_tab;
