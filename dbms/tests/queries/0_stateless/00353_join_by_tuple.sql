set any_join_distinct_right_table_keys = 1;
select a from (select (1, 2) as a) any inner join (select (1, 2) as a) using a;
