/**
 * Benchmark: zstd compression level vs. throughput, ratio, and latency for Raft log entries.
 *
 * Run with:
 *   ./build/src/rk_unit_tests --gtest_filter='ZstdLevelBench.*' 2>&1 | grep -E 'level|MB|ratio'
 *
 * Output columns (one row per level, plus a "none" baseline):
 *   level | write_MB/s | read_MB/s | ratio | p50_us | p99_us
 */
#include <algorithm>
#include <cstring>
#include <numeric>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>

#include <Common/Stopwatch.h>
#include <Service/NuRaftFileLogStore.h>
#include <Service/tests/raft_test_common.h>
#include <zstd.h>

using namespace nuraft;
using namespace RK;

namespace
{

static constexpr int BENCH_ENTRY_COUNT = 1000000;
static constexpr int KEY_BYTES = 64;

/// Build a payload that resembles real ZK node data: mix of repetitive + random-ish bytes.
/// Pure-random data defeats compression benchmarks; this is more realistic.
String makeBenchData(int value_bytes)
{
    String s;
    s.reserve(value_bytes);
    // 75% repetitive (JSON-like schema), 25% pseudo-random
    int rep = value_bytes * 3 / 4;
    for (int i = 0; i < rep; i++)
        s += "abcdefghijklmnopqrstuvwxyz0123456789"[i % 36];
    for (int i = rep; i < value_bytes; i++)
        s += static_cast<char>((i * 31 + 7) & 0x7f);
    return s;
}

struct BenchResult
{
    int level;          // -1 = no compression (baseline)
    double write_mbs;   // uncompressed MB/s written
    double read_mbs;    // uncompressed MB/s read back
    double ratio;       // uncompressed / on-disk size  (>1 = compression win)
    double p50_us;      // per-entry write latency p50 microseconds
    double p99_us;      // per-entry write latency p99 microseconds
    UInt64 disk_bytes;
    UInt64 raw_bytes;
};

BenchResult runLevel(int level /* -1=raw */, const String & key, const String & data, int value_bytes, int batch_size = 1, bool sync_per_batch = false)
{
    String log_dir = String("./test_raft_log/zstd_bench_level_") + (level < 0 ? "none" : std::to_string(level))
        + "_b" + std::to_string(batch_size) + (sync_per_batch ? "_sync" : "");
    cleanDirectory(log_dir);

    LogEntryCodec codec = (level < 0) ? LogEntryCodec::RAW : LogEntryCodec::ZSTD;

    /// If zstd, temporarily override the level via env — simplest without plumbing level into codec yet.
    /// We instead measure at the LogSegmentStore layer which calls ZstdLogCodec internally.
    /// For level variants we directly exercise ZstdLogCodec::compress at different levels here,
    /// measuring raw codec throughput, then measure end-to-end write/read via LogSegmentStore.

    // ── 1. Per-entry latency samples via direct codec calls ──────────────────
    std::vector<double> latencies_us;
    latencies_us.reserve(BENCH_ENTRY_COUNT);
    UInt64 compressed_total = 0;
    UInt64 raw_total = 0;

    String payload = data; // value_bytes of bench data
    raw_total = static_cast<UInt64>(payload.size()) * BENCH_ENTRY_COUNT;

    if (level >= 0)
    {
        for (int i = 0; i < BENCH_ENTRY_COUNT; i++)
        {
            Stopwatch sw;
            sw.start();
            size_t bound = ZSTD_compressBound(payload.size());
            std::vector<char> buf(bound);
            size_t written = ZSTD_compress(buf.data(), bound, payload.data(), payload.size(), level);
            sw.stop();
            latencies_us.push_back(sw.elapsedMicroseconds());
            compressed_total += ZSTD_isError(written) ? payload.size() : written;
        }
    }
    else
    {
        for (int i = 0; i < BENCH_ENTRY_COUNT; i++)
        {
            Stopwatch sw;
            sw.start();
            // "no-op" copy to measure overhead baseline
            volatile char sink = payload[payload.size() / 2];
            (void)sink;
            sw.stop();
            latencies_us.push_back(sw.elapsedMicroseconds());
            compressed_total += payload.size();
        }
    }

    std::sort(latencies_us.begin(), latencies_us.end());
    double p50 = latencies_us[latencies_us.size() / 2];
    double p99 = latencies_us[latencies_us.size() * 99 / 100];

    // ── 2. End-to-end write throughput via LogSegmentStore ───────────────────
    auto store = LogSegmentStore::getInstance(log_dir, true,
        LogSegmentStore::MAX_LOG_SEGMENT_FILE_SIZE, codec);
    store->init();

    Stopwatch write_watch;
    write_watch.start();
    for (int i = 0; i < BENCH_ENTRY_COUNT; i += batch_size)
    {
        int n = std::min(batch_size, BENCH_ENTRY_COUNT - i);
        for (int j = 0; j < n; j++)
            appendEntry(store, 1, const_cast<String &>(key), const_cast<String &>(data));
        if (sync_per_batch)
            store->flush(); // simulates FSYNC_PARALLEL fsyncThread waking on end_of_append_batch
    }
    write_watch.stop();

    // ── 3. End-to-end read throughput ────────────────────────────────────────
    Stopwatch read_watch;
    read_watch.start();
    for (int i = 1; i <= BENCH_ENTRY_COUNT; i++)
    {
        auto e = store->getEntry(i);
        (void)e;
    }
    read_watch.stop();

    store->close();

    // Measure actual disk usage
    UInt64 disk_bytes_used = 0;
    for (const auto & seg : store->getClosedSegments())
        disk_bytes_used += seg->getFileSize();
    // open segment
    // (getClosedSegments only — approximate, open segment omitted for simplicity)

    double uncompressed_mb = static_cast<double>((KEY_BYTES + value_bytes) * BENCH_ENTRY_COUNT) / 1e6;
    double write_ms = write_watch.elapsedMilliseconds();
    double read_ms = read_watch.elapsedMilliseconds();

    cleanDirectory(log_dir);

    BenchResult r;
    r.level = level;
    r.write_mbs = (write_ms > 0) ? uncompressed_mb / write_ms * 1000.0 : 0;
    r.read_mbs = (read_ms > 0) ? uncompressed_mb / read_ms * 1000.0 : 0;
    r.ratio = (compressed_total > 0) ? static_cast<double>(raw_total) / compressed_total : 1.0;
    r.p50_us = p50;
    r.p99_us = p99;
    r.disk_bytes = compressed_total;
    r.raw_bytes = raw_total;
    return r;
}

} // namespace


/// DISABLED_ prefix: this is a manual perf benchmark (1M entries x many configs),
/// not a correctness test. gtest skips it by default in every CI job regardless of
/// filter. Run explicitly with:
///   ./rk_unit_tests --gtest_also_run_disabled_tests --gtest_filter='ZstdLevelBench.*'
TEST(ZstdLevelBench, DISABLED_AllLevels)
{
    /// Three payload sizes to show how compressibility affects the sweet spot.
    struct PayloadCase { int value_bytes; const char * label; };
    std::vector<PayloadCase> cases = {
        {128,  "128B "},
        {307,  "0.3KB"},
        {1024, "1KB  "},
    };

    /// Levels to benchmark: -1=none, then 1..9.
    std::vector<int> levels = {-1, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    String key(KEY_BYTES, 'k');

    for (auto & pc : cases)
    {
        String data = makeBenchData(pc.value_bytes);

        fprintf(stderr, "\n━━━━ payload=%s ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n", pc.label);
        fprintf(stderr, "%-8s %12s %12s %8s %10s %10s\n",
            "level", "write MB/s", "read MB/s", "ratio", "p50 us", "p99 us");
        fprintf(stderr, "%s\n", std::string(70, '-').c_str());

        for (int lvl : levels)
        {
            auto r = runLevel(lvl, key, data, pc.value_bytes);
            String level_str = (lvl < 0) ? "none" : std::to_string(lvl);
            fprintf(stderr, "%-8s %12.1f %12.1f %8.2f %10.1f %10.1f\n",
                level_str.c_str(), r.write_mbs, r.read_mbs, r.ratio, r.p50_us, r.p99_us);
        }
    }

    // ── Batch-size sweep: none vs zstd3, with fsync per batch ──────────────────
    std::vector<int> batch_sizes = {1, 10, 100};

    fprintf(stderr, "\n━━━━ batch sweep with fsync (none vs zstd3) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    fprintf(stderr, "%-6s %-8s %12s %12s %8s\n",
        "batch", "codec", "write MB/s", "read MB/s", "ratio");
    fprintf(stderr, "%s\n", std::string(52, '-').c_str());

    for (auto & pc : cases)
    {
        String data = makeBenchData(pc.value_bytes);
        for (int bs : batch_sizes)
        {
            auto r_none = runLevel(-1, key, data, pc.value_bytes, bs, /*sync_per_batch=*/true);
            fprintf(stderr, "%-6d %-8s %12.1f %12.1f %8.2f\n",
                bs, "none", r_none.write_mbs, r_none.read_mbs, r_none.ratio);

            auto r_zstd = runLevel(3, key, data, pc.value_bytes, bs, /*sync_per_batch=*/true);
            fprintf(stderr, "%-6d %-8s %12.1f %12.1f %8.2f\n",
                bs, "zstd3", r_zstd.write_mbs, r_zstd.read_mbs, r_zstd.ratio);
        }
        fprintf(stderr, "\n");
    }

    /// The test always passes — it's a benchmark, not a correctness check.
    SUCCEED();
}
