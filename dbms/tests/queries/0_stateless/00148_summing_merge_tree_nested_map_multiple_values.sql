drop table if exists test.nested_map_multiple_values;

create table test.nested_map_multiple_values (d materialized today(), k UInt64, payload materialized rand(), SomeMap Nested(ID UInt32, Num1 Int64, Num2 Float64)) engine=SummingMergeTree(d, k, 8192);

insert into test.nested_map_multiple_values values (0,[1],[100],[1.0]),(1,[1],[100],[1.0]),(2,[1],[100],[1.0]),(3,[1,2],[100,150],[1.0,1.5]);
insert into test.nested_map_multiple_values values (0,[2],[150],[-2.5]),(1,[1],[150],[-1.0]),(2,[1,2],[150,150],[2.5,3.5]),(3,[1],[-100],[-1]);
optimize table test.nested_map_multiple_values;
select * from test.nested_map_multiple_values;

drop table test.nested_map_multiple_values;

drop table if exists test.nested_not_a_map;
create table test.nested_not_a_map (d materialized today(), k UInt64, payload materialized rand(), OnlyOneColumnMap Nested(ID UInt32), NonArithmeticValueMap Nested(ID UInt32, Date Date), Nested_ Nested(ID UInt32, Num Int64)) engine=SummingMergeTree(d, k, 8192);

insert into test.nested_not_a_map values (0,[1],[1],['2015-04-09'],[1],[100]);
insert into test.nested_not_a_map values (0,[1],[1],['2015-04-08'],[1],[200]);
select * from test.nested_not_a_map;

optimize table test.nested_not_a_map;
select * from test.nested_not_a_map;

drop table test.nested_not_a_map;
