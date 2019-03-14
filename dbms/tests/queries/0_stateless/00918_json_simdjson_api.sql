select jsonLength('{"a": "hello", "b": [-100, 200.0, 300]}');
select jsonType('{"a": "hello", "b": [-100, 200.0, 300]}');
select jsonHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
select jsonHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
select jsonExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1);
select jsonExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 2);
select jsonExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a');
select jsonLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
select jsonType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b');
select jsonExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1);
select jsonExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2);
select jsonExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1);
select jsonExtract('{"a": "hello", "b": [-100, 200.0, 300]}', ('', '', '', [0.0]));
select jsonExtract('{"a": "hello", "b": [-100, 200.0, 300]}', [-0], 'b');
select jsonExtract('{"a": "hello", "b": [-100, 200.0, 300]}', ['']);
select jsonExtract('{"a": "hello", "b": [-100, 200.0, 300]}', [(-0, 0.0, 0)]);
