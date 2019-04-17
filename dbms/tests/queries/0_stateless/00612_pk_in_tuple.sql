create database if not exists test;
drop table if exists tab;
create table tab (key UInt64, arr Array(UInt64)) Engine = MergeTree order by key;
insert into tab values (1, [1]);
insert into tab values (2, [2]);
select 'all';
select * from tab order by key;
select 'key, arrayJoin(arr) in (1, 1)';
select key, arrayJoin(arr) as val from tab where (key, val) in (1, 1);
select 'key, arrayJoin(arr) in ((1, 1), (2, 2))';
select key, arrayJoin(arr) as val from tab where (key, val) in ((1, 1), (2, 2)) order by key;
select '(key, left array join arr) in (1, 1)';
select key from tab left array join arr as val where (key, val) in (1, 1);
select '(key, left array join arr) in ((1, 1), (2, 2))';
select key from tab left array join arr as val where (key, val) in ((1, 1), (2, 2)) order by key;

drop table if exists tab;
create table tab (key UInt64, n Nested(x UInt64)) Engine = MergeTree order by key;
insert into tab values (1, [1]);
insert into tab values (2, [2]);
select 'all';
select * from tab order by key;
select 'key, arrayJoin(n.x) in (1, 1)';
select key, arrayJoin(n.x) as val from tab where (key, val) in (1, 1);
select 'key, arrayJoin(n.x) in ((1, 1), (2, 2))';
select key, arrayJoin(n.x) as val from tab where (key, val) in ((1, 1), (2, 2)) order by key;
select '(key, left array join n.x) in (1, 1)';
select key from tab left array join n.x as val where (key, val) in (1, 1);
select '(key, left array join n.x) in ((1, 1), (2, 2))';
select key from tab left array join n.x as val where (key, val) in ((1, 1), (2, 2)) order by key;
select 'max(key) from tab where (key, left array join n.x) in (1, 1)';
select max(key) from tab left array join `n.x` as val where (key, val) in ((1, 1));
select max(key) from tab left array join n as val where (key, val.x) in (1, 1);
select 'max(key) from tab where (key, left array join n.x) in ((1, 1), (2, 2))';
select max(key) from tab left array join `n.x` as val where (key, val) in ((1, 1), (2, 2));
select max(key) from tab left array join n as val where (key, val.x) in ((1, 1), (2, 2));
select 'max(key) from tab any left join (select key, arrayJoin(n.x) as val from tab) using key where (key, val) in (1, 1)';
select max(key) from tab any left join (select key, arrayJoin(n.x) as val from tab) using key where (key, val) in (1, 1);
select 'max(key) from tab any left join (select key, arrayJoin(n.x) as val from tab) using key where (key, val) in ((1, 1), (2, 2))';
select max(key) from tab any left join (select key, arrayJoin(n.x) as val from tab) using key where (key, val) in ((1, 1), (2, 2));

drop  table if exists tab;
CREATE TABLE tab (key1 Int32, id1  Int64, c1 Int64) ENGINE = MergeTree  PARTITION BY id1 ORDER BY (key1) ;
insert into tab values ( -1, 1, 0 );
SELECT count(*) FROM  tab PREWHERE id1 IN (1);
