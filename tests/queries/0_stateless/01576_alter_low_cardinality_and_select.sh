#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS alter_table"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE alter_table (key UInt64, value String) ENGINE MergeTree ORDER BY key"

# we don't need mutations and merges
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES alter_table"

${CLICKHOUSE_CLIENT} --query "INSERT INTO alter_table SELECT number, toString(number) FROM numbers(10000)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO alter_table SELECT number, toString(number) FROM numbers(10000, 10000)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO alter_table SELECT number, toString(number) FROM numbers(20000, 10000)"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM alter_table WHERE value == '733'"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE alter_table MODIFY COLUMN value LowCardinality(String)" &

type_query="SELECT type FROM system.columns WHERE name = 'value' and table='alter_table' and database='${CLICKHOUSE_DATABASE}'"
value_type=""

# waiting until schema will change (but not data)
while [[ "$value_type" != "LowCardinality(String)" ]]
do
    sleep 0.1
    value_type=$($CLICKHOUSE_CLIENT --query "$type_query")
done

# checking type is LowCardinalty
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE alter_table"

# checking no mutations happened
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.parts where table='alter_table' and active and database='${CLICKHOUSE_DATABASE}' ORDER BY name"

# checking that conversions applied "on fly" works
${CLICKHOUSE_CLIENT} --query "SELECT * FROM alter_table PREWHERE key > 700 WHERE value = '701'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS alter_table"
