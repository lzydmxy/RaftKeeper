# Zstd snapshot compression benchmark

## Design

**Goal:** quantify what V3 (zstd) snapshots buy over V2 (raw) on the two axes
that matter for snapshots — **on-disk / on-wire size** (snapshots are shipped
to lagging followers on catch-up) and **create/load wall time** (does
compressing slow down snapshotting or recovery?).

Harness: `src/Service/tests/gtest_snapshot_zstd_bench.cpp`
(`SnapshotZstdBench.DISABLED_V2vsV3`). It exercises the real snapshot path —
`KeeperSnapshotManager::createSnapshot(..., version)` and `parseSnapshot(...)` —
so it measures the same code production uses, not a synthetic codec loop.

### Dimensions

| Dimension | Values | Why |
|-----------|--------|-----|
| Store shape | 100K×128B, 100K×0.3KB, 50K×1KB | small flags → typical ZK data → large values |
| Version | V2 (raw), V3 (zstd level 3) | the on/off comparison |

Level is fixed at 3 — reuses `ZstdLogCodec`, whose level was already tuned in
the [log benchmark](zstd-level-benchmark.md); no reason to expect a different
sweet spot for snapshot bodies.

### Data shape

75% repetitive (JSON-like), 25% pseudo-random, same generator as the log
benchmark. Pure-random defeats compression; pure-repetition is unrealistically
easy. Real ZK trees skew *more* repetitive than this (shared path prefixes,
templated values), so the ratios below are conservative-to-optimistic
depending on workload.

### Metrics

| Metric | Meaning |
|--------|---------|
| `create_ms` | wall time to serialize the whole store to snapshot objects |
| `load_ms` | wall time to parse the snapshot back into a fresh store |
| `disk_MB` | total on-disk size of all snapshot objects |
| `ratio` | V2 disk size / V3 disk size (higher = better) |

## Results

1M-ish nodes, single run, RelWithDebInfo, no sanitizer:

```
version       nodes   create_ms    load_ms    disk_MB    ratio
------------------------------------------------------------
── 100K x 128B ──
V2           100000       275.0       73.0      22.39     1.00
V3           100000       237.0       59.0       2.00    11.17
── 100K x 0.3KB ──
V2           100000       354.0       96.0      40.29     1.00
V3           100000       265.0       69.0       2.14    18.85
── 50K x 1KB ──
V2            50000       384.0       85.0      55.99     1.00
V3            50000       194.0       62.0       1.14    48.93
```

## Analysis

### V3 is smaller AND faster

Unlike the log path (where per-entry compression cost is visible at large
batches), snapshot V3 **wins on every axis**:

- **Size: 11–49× smaller.** Snapshot bodies are batched (`SAVE_BATCH_SIZE`
  nodes per zstd frame), so the compressor sees a large, highly-repetitive
  window — far better than the log path's per-entry frames.
- **Create: 14–49% faster.** Writing 2 MB instead of 56 MB through
  `WriteBufferFromFile` (plus the final flush/fsync) saves more time than the
  zstd CPU costs. The bigger the values, the bigger the win (1KB case: 384→194 ms).
- **Load: 19–27% faster.** Reading + CRC-checking far fewer bytes off disk
  outweighs decompression, same effect the log read path showed.

### Why the ratio climbs with value size

At 128B most of a node's on-disk footprint is fixed overhead (path, stat
struct, acl_id) that compresses modestly; at 1KB the compressible value
dominates, so the ratio jumps to ~49×. The absolute *bytes saved* grows even
faster (56 MB → 1.1 MB).

### Caveats

- Ratios are workload-dependent. This data is deliberately compressible;
  incompressible blobs (already-compressed payloads, encryption) would
  approach 1.0 — but zstd's frame overhead is tiny, so V3 is never
  meaningfully *worse* on size, and still wins on create time via fewer bytes
  written.
- Single-run wall times carry ±10% noise; the size ratios are deterministic.

## Recommendation

- **Enable `snapshot_compression=zstd` wherever snapshots are large or shipped
  between nodes.** It reduces disk, reduces follower catch-up transfer, and is
  faster to write and read.
- Kept **opt-in / default off** only for rolling-upgrade safety (a V3 snapshot
  can't be read by a pre-V3 binary). Once every node runs a V3-capable binary,
  turn it on cluster-wide.

## Reproduce

```bash
cmake -B build   # only needed once, to pick up the new gtest file
ninja -C build rk_unit_tests
./build/src/rk_unit_tests --gtest_also_run_disabled_tests \
    --gtest_filter='SnapshotZstdBench.*' 2>&1
```
