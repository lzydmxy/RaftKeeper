set output_format_write_statistics = 0;
select g, sum(number) from numbers(4) group by bitAnd(number, 1) as g with totals having sum(number) <= arrayJoin([2, 4]) format Pretty;
select '-';
select g, s from (select g, sum(number) as s from numbers(4) group by bitAnd(number, 1) as g with totals) array join [1, 2] as a format Pretty;
select '--';

select g, sum(number) from numbers(4) group by bitAnd(number, 1) as g with totals having sum(number) <= arrayJoin([2, 4]) format TSV;
select '-';
select g, s from (select g, sum(number) as s from numbers(4) group by bitAnd(number, 1) as g with totals) array join [1, 2] as a format TSV;
select '--';

select g, sum(number) from numbers(4) group by bitAnd(number, 1) as g with totals having sum(number) <= arrayJoin([2, 4]) format JSON;
select '-';
select g, s from (select g, sum(number) as s from numbers(4) group by bitAnd(number, 1) as g with totals) array join [1, 2] as a format JSON;
select '--';
