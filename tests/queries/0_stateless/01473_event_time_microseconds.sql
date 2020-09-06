-- This file contains tests for the event_time_microseconds field for various tables.
-- Note: Only event_time_microseconds for asynchronous_metric_log table is tested via
-- an integration test as those metrics take 60s by default to be updated.
-- Refer: tests/integration/test_asynchronous_metric_log_table.

set log_queries = 1;

select '01473_metric_log_table_event_start_time_microseconds_test';
system flush logs;
SELECT sleep(3) Format Null;
SELECT If((select count(event_time_microseconds)  from system.metric_log) > 0, 'ok', 'fail'); -- success
