CREATE TABLE local_table
(
    id Int32,
    name String,
    ts DateTime,
    oth_id Int32
) ENGINE = MergeTree() PARTITION BY toMonday(ts) ORDER BY (ts, id);

CREATE TABLE other_table
(
    id Int32,
    name String,
    ts DateTime,
    trd_id Int32
) ENGINE = MergeTree() PARTITION BY toMonday(ts) ORDER BY (ts, id);

INSERT INTO local_table VALUES(1, 'One', now(), 100);
INSERT INTO local_table VALUES(2, 'Two', now(), 200);
INSERT INTO other_table VALUES(100, 'One Hundred', now(), 1000);
INSERT INTO other_table VALUES(200, 'Two Hundred', now(), 2000);

-- FIXME: other_table -> other_table in first query breaks test (external tables cache error)
select t2.name from remote('127.0.0.2', default, 'local_table') as t1
left join other_table as t2
on t1.oth_id = t2.id
order by t2.name;

select t2.name from other_table as t2
global right join remote('127.0.0.2', default, 'local_table') as t1
on t1.oth_id = t2.id
order by t2.name;

select t2.name from remote('127.0.0.2', default, 'local_table') as t1
global left join other_table as t2
on t1.oth_id = t2.id
order by t2.name;

select t2.name from remote('127.0.0.2', default, 'local_table') as t1
global left join other_table as t2
on t1.oth_id = t2.id
order by t2.name;

select other_table.name from remote('127.0.0.2', default, 'local_table') as t1
global left join other_table
on t1.oth_id = other_table.id
order by other_table.name;

select other_table.name from remote('127.0.0.2', default, 'local_table') as t1
global left join other_table as t2
on t1.oth_id = other_table.id
order by other_table.name;
