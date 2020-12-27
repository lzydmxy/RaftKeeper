--
-- byteSize
--
select '';
select '# byteSize';

set allow_experimental_bigint_types = 1;

-- numbers #0 --
select '';
select 'byteSize for numbers #0';
drop table if exists test_byte_size_number0;
create table test_byte_size_number0
(
    key Int32,
    u8 UInt8,
    u16 UInt16,
    u32 UInt32,
    u64 UInt64,
    u256 UInt256,
    i8 Int8,
    i16 Int16,
    i32 Int32,
    i64 Int64,
    i128 Int128,
    i256 Int256,
    f32 Float32,
    f64 Float64
) engine MergeTree order by key;

insert into test_byte_size_number0 values(1, 8, 16, 32, 64, 256, -8, -16, -32, -64, -128, -256, 32.32, 64.64);
insert into test_byte_size_number0 values(2, 8, 16, 32, 64, 256, -8, -16, -32, -64, -128, -256, 32.32, 64.64);

select key, toTypeName(u8),byteSize(u8), toTypeName(u16),byteSize(u16), toTypeName(u32),byteSize(u32), toTypeName(u64),byteSize(u64), toTypeName(u256),byteSize(u256) from test_byte_size_number0 order by key;
select key, toTypeName(i8),byteSize(i8), toTypeName(i16),byteSize(i16), toTypeName(i32),byteSize(i32), toTypeName(i64),byteSize(i64), toTypeName(i128),byteSize(i128), toTypeName(u256),byteSize(u256) from test_byte_size_number0 order by key;
select key, toTypeName(f32),byteSize(f32), toTypeName(f64),byteSize(f64) from test_byte_size_number0 order by key;

drop table if exists test_byte_size_number0;


-- numbers #1 --
select '';
select 'byteSize for numbers #1';
drop table if exists test_byte_size_number1;
create table test_byte_size_number1
(
    key Int32,
    date Date,
    dt DateTime,
    dt64 DateTime64(3),
    en8 Enum8('a'=1, 'b'=2, 'c'=3, 'd'=4),
    en16 Enum16('c'=100, 'l'=101, 'i'=102, 'ck'=103, 'h'=104, 'o'=105, 'u'=106, 's'=107, 'e'=108),
    dec32 Decimal32(4),
    dec64 Decimal64(8),
    dec128 Decimal128(16),
    dec256 Decimal256(16),
    uuid UUID
) engine MergeTree order by key;

insert into test_byte_size_number1 values(1, '2020-01-01', '2020-01-01 01:02:03', '2020-02-02 01:02:03', 'a', 'ck', 32.32, 64.64, 128.128, 256.256, generateUUIDv4());
insert into test_byte_size_number1 values(2, '2020-01-01', '2020-01-01 01:02:03', '2020-02-02 01:02:03', 'a', 'ck', 32.32, 64.64, 128.128, 256.256, generateUUIDv4());

select key,byteSize(*), toTypeName(date),byteSize(date), toTypeName(dt),byteSize(dt), toTypeName(dt64),byteSize(dt64), toTypeName(uuid),byteSize(uuid) from test_byte_size_number1 order by key;

drop table if exists test_byte_size_number1;


-- string --
select '';
select 'byteSize for strings';
drop table if exists test_byte_size_string;
create table test_byte_size_string
(
    key Int32,
    str1 String,
    str2 String,
    fstr1 FixedString(8),
    fstr2 FixedString(8)
) engine MergeTree order by key;

insert into test_byte_size_string values(1, '', 'a', '', 'abcde');
insert into test_byte_size_string values(2, 'abced', '', 'abcde', '');

select key,byteSize(*), str1,byteSize(str1), str2,byteSize(str2), fstr1,byteSize(fstr1), fstr2,byteSize(fstr2) from test_byte_size_string order by key;

drop table if exists test_byte_size_string;


-- array --
drop table if exists test_byte_size_array;
create table test_byte_size_array
(
    key Int32,
    ints Array(Int32),
    int_ints Array(Array(Int32)),
    strs Array(String),
    str_strs Array(Array(String))
) engine MergeTree order by key;

insert into test_byte_size_array values(1, [], [[]], [], [[]]);
insert into test_byte_size_array values(2, [1,2], [[], [1,2]], [''], [[], ['']]);
insert into test_byte_size_array values(3, [0,256], [[], [1,2], [0,256]], ['','a'], [[], [''], ['','a']]);
insert into test_byte_size_array values(4, [256,65536], [[], [1,2], [0,256], [256,65536]], ['','a','abced'], [[], [''], ['','a'], ['','a','abced']]);

select '';
select 'byteSize for int arrays';
select key,byteSize(*), ints,byteSize(ints), int_ints,byteSize(int_ints) from test_byte_size_array order by key;

select '';
select 'byteSize for string arrays';
select key,byteSize(*), strs,byteSize(strs), str_strs,byteSize(str_strs) from test_byte_size_array order by key;
-- select key, int_ints,byteSize(int_ints) from test_byte_size_array order by key;

drop table if exists test_byte_size_array;
