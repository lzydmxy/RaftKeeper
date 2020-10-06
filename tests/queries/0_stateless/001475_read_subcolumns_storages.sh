#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

create_query="CREATE TABLE subcolumns(n Nullable(UInt32), a1 Array(UInt32),\
    a2 Array(Array(Array(UInt32))), a3 Array(Nullable(UInt32)), t Tuple(s String, v UInt32))"

declare -a ENGINES=("Log" "StripeLog" "TinyLog" "Memory" \
    "MergeTree ORDER BY tuple() SETTINGS min_bytes_for_compact_part='10M'"
    "MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part='10M'"
    "MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0")

for engine in "${ENGINES[@]}"; do
    echo $engine
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS subcolumns"
    $CLICKHOUSE_CLIENT --query "$create_query ENGINE = $engine"
    $CLICKHOUSE_CLIENT --query "INSERT INTO subcolumns VALUES (100, [1, 2, 3], [[[1, 2], [], [4]], [[5, 6], [7, 8]], [[]]], [1, NULL, 2], ('foo', 200))"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM subcolumns"
    $CLICKHOUSE_CLIENT --query "SELECT n, n.null, a1, a1.size0, a2, a2.size0, a2.size1, a2.size2, a3, a3.size0, a3.null, t, t.s, t.v FROM subcolumns"
done
