SELECT count(), uniq(dummy) FROM remote('localhost,127.0.0.{1,2}', system.one) SETTINGS distributed_group_by_no_merge = 1;
