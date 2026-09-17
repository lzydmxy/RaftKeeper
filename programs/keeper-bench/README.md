# RaftKeeper benchmark

`raftkeeper keeper-bench` generates a synthetic ZooKeeper workload against one
or more RaftKeeper/ZooKeeper-compatible endpoints.

## Unified runner

`bench.py` is the recommended entry point. It owns prefill, workload execution,
Keeper monitoring, optional snapshot triggering, and result artifacts.

```bash
python3 programs/keeper-bench/bench.py \
    --profile production \
    --servers host1:2181 host2:2181 host3:2181 \
    --bench-path /clickhouse_rmt_bench \
    --root-name production-state \
    --output-dir benchmark-results/production-state
```

The production profile initializes about 3.75 million znodes, runs 30k reads/s
plus 15k writes/s for ten minutes, grows the tree by about 3,600 nodes/s, keeps
overdue requests, and triggers a snapshot at five minutes. Each output directory
contains:

- `manifest.json`: resolved parameters and exact commands;
- `setup.log`: prefill output;
- `benchmark.log`: workload output;
- `keeper-metrics.csv`: one-second Keeper samples;
- `state-before.json` and `state-after.json`: endpoint state snapshots.
- `report.json` and `report.md`: parsed benchmark and peak-interval summary.

Use `--profile smoke` for a small validation. Use `--reuse-existing` with an
explicit `--root-name` to skip prefill for repeated A/B tests. Add
`--drop-late-requests` when a capacity test should shed overdue offered load
instead of modeling production backlog.

```bash
./build/programs/raftkeeper keeper-bench \
    --server 127.0.0.1:8101 \
    --concurrency 10 \
    --pipeline-depth 16 \
    --duration-sec 60 \
    --operations create:10 set:80 get:450 list:450
```

`--concurrency` controls worker threads. `--pipeline-depth` controls the maximum
number of asynchronous requests in flight in each worker. The maximum total
number of in-flight requests is therefore `concurrency * pipeline-depth`.

Operation counts are executed in the order given and repeated until the time
limit is reached. `get` and `set` choose randomly among nodes created earlier in
the current cycle. One seed node is created for every cycle, so read-only
workloads are valid. Setup requests and optional cleanup requests are not
included in the reported operation count or latency distribution.

The final report contains successful request throughput and latency in
microseconds for all, read, and write requests. Failed responses are reported
separately. The benchmark waits until every local worker is ready before it
starts timing. For coordinated runs from multiple processes, set the same
`--bench-path` and `--bench-cnt` in each process.

Generated benchmark nodes are retained under the path printed at the end of the
run. Set `--delete-create-node true` to remove completed operation cycles; the
benchmark root, worker nodes, an interrupted final cycle, and result nodes are
still retained.

## ClickHouse ReplicatedMergeTree workload

The built-in ClickHouse workload models the request shape observed during
ReplicatedMergeTree insert, fetch, and merge bursts:

- reads are 65% get, 33% list, and 2% exists;
- writes are Multi requests containing create, set, and remove subrequests;
- every metadata group has a `blocks` path and two replica `parts` paths;
- each list path has a configurable number of children.

Requests are paced against a fixed offered rate, so throughput improvements do
not silently increase the load during comparisons.

```bash
./build/programs/raftkeeper keeper-bench \
    --server host1:2181 host2:2181 host3:2181 \
    --bench-path /clickhouse_rmt_bench \
    --clickhouse-rmt-workload true \
    --shared-keeper false \
    --concurrency 226 \
    --pipeline-depth 16 \
    --target-read-qps 30000 \
    --target-write-qps 15000 \
    --target-create-qps 3600 \
    --metadata-groups 32 \
    --children-per-list 1000 \
    --multi-size 5 \
    --workload-root-name rmt-production-profile \
    --drop-late-requests false \
    --operation-timeout-ms 35000 \
    --session-timeout-ms 3600000 \
    --duration-sec 120
```

`metadata-groups * (3 * children-per-list + 21)` approximates the generated
znode count. Increase `metadata-groups` for snapshot tests; keep
`children-per-list` near production values so list response sizes remain
representative. `target-create-qps` is part of `target-write-qps`; those
requests create persistent part-like nodes and therefore grow the tree. The
generated tree is not automatically removed.

For a production znode-growth profile, use `--metadata-groups 1242` to create
approximately 3.75 million initial nodes, then set
`--target-create-qps 3600 --duration-sec 600` to add approximately 2.16 million
part-like nodes in ten minutes. `target-create-qps` must not exceed
`target-write-qps`.

To repeat a test against the same initialized tree, provide the same
`--bench-path` and `--workload-root-name` together with `--skip-setup true`.

By default, workers discard offered-load slots that are already overdue. Set
`--drop-late-requests false` to model ClickHouse-style backlog: after a Keeper
stall, workers retain schedule debt and continue sending until they catch up.
Use a larger `--pipeline-depth` for this mode. The final report includes dropped
slots and maximum schedule lag. Set operation/session timeouts to the production
client values; otherwise a deliberate backlog test can expire at the default
operation timeout before its latency is measured.

Use `monitor_keeper.py` to record one-second `mntr` samples and optionally
trigger a snapshot during the run:

```bash
python3 programs/keeper-bench/monitor_keeper.py \
    --servers host1:2181 host2:2181 host3:2181 \
    --duration 60 \
    --interval 1 \
    --reset-stats \
    --snapshot-at 20 \
    --snapshot-server host2:2181 \
    --output keeper-metrics.csv
```

`--reset-stats` sends the `srst` four-letter command and must only be used on a
dedicated benchmark cluster. `--snapshot-server` must identify the leader.

The monitor can also own the benchmark process. It waits for the benchmark's
`Run benchmark for` marker, then resets statistics and starts sampling, so setup
traffic is excluded automatically. Arguments after `--` are passed directly to
the benchmark:

```bash
python3 programs/keeper-bench/monitor_keeper.py \
    --servers host1:2181 host2:2181 host3:2181 \
    --duration 120 \
    --reset-stats \
    --snapshot-at 40 \
    --output keeper-metrics.csv \
    --benchmark-output keeper-bench.log \
    -- ./build/programs/raftkeeper keeper-bench <benchmark options>
```

The wrapper streams benchmark output, stops monitoring when the benchmark exits,
and returns the benchmark's exit status.
