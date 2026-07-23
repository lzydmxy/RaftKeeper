/**
 * Benchmark: zstd snapshot compression (V2 raw vs V3 zstd) for RaftKeeper.
 *
 * Snapshots are the largest on-disk artifact and are transferred to lagging
 * followers on catch-up, so the metrics that matter are on-disk size (network
 * + disk) and create/load wall time (does compression slow snapshotting?).
 *
 * Run with:
 *   ./build/src/rk_unit_tests --gtest_also_run_disabled_tests \
 *       --gtest_filter='SnapshotZstdBench.*' 2>&1
 *
 * Output columns (one row per version, per store shape):
 *   version | nodes | create_ms | load_ms | disk_MB | ratio
 */
#include <string>
#include <vector>

#include <Poco/File.h>
#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>

#include <Common/Stopwatch.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/tests/raft_test_common.h>

using namespace nuraft;
using namespace RK;

namespace
{

/// Build a payload resembling real ZK node data: mostly repetitive with a
/// pseudo-random tail. Pure-random defeats compression; pure-repetition is
/// unrealistically easy. Same shape as gtest_zstd_bench.cpp for comparability.
String makeBenchData(int value_bytes, int salt)
{
    String s;
    s.reserve(value_bytes);
    int rep = value_bytes * 3 / 4;
    for (int i = 0; i < rep; i++)
        s += "abcdefghijklmnopqrstuvwxyz0123456789"[(i + salt) % 36];
    for (int i = rep; i < value_bytes; i++)
        s += static_cast<char>((i * 31 + 7 + salt) & 0x7f);
    return s;
}

UInt64 dirSizeBytes(const String & dir)
{
    UInt64 total = 0;
    std::vector<String> files;
    Poco::File(dir).list(files);
    for (const auto & f : files)
        total += Poco::File(dir + "/" + f).getSize();
    return total;
}

struct BenchResult
{
    SnapshotVersion version;
    int nodes;
    double create_ms;
    double load_ms;
    UInt64 disk_bytes;
    double ratio; /// disk size of V2 baseline / this version's disk size
};

BenchResult runOne(SnapshotVersion version, int node_count, int value_bytes, int idx)
{
    String snap_dir = SNAP_DIR + "/zstd_bench_" + toString(version) + "_" + std::to_string(idx);
    cleanDirectory(snap_dir);

    KeeperSnapshotManager snap_mgr(snap_dir, 3, 10000);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);

    RaftSettingsPtr raft_settings(RaftSettings::getDefault());
    KeeperStore store(raft_settings->dead_session_check_period_ms);

    for (int i = 0; i < node_count; i++)
        setNode(store, std::to_string(i), makeBenchData(value_bytes, i));

    snapshot meta(node_count, 1, config);

    Stopwatch create_watch;
    create_watch.start();
    snap_mgr.createSnapshot(meta, store, store.getZxid(), store.getSessionIDCounter(), version);
    create_watch.stop();

    UInt64 disk = dirSizeBytes(snap_dir);

    KeeperStore new_store(raft_settings->dead_session_check_period_ms);
    Stopwatch load_watch;
    load_watch.start();
    snap_mgr.parseSnapshot(meta, new_store);
    load_watch.stop();

    /// sanity: loaded store must match
    EXPECT_EQ(new_store.getNodesCount(), store.getNodesCount());

    cleanDirectory(snap_dir);

    BenchResult r;
    r.version = version;
    r.nodes = node_count;
    r.create_ms = create_watch.elapsedMilliseconds();
    r.load_ms = load_watch.elapsedMilliseconds();
    r.disk_bytes = disk;
    r.ratio = 1.0;
    return r;
}

} // namespace


/// DISABLED_ prefix: manual perf benchmark, skipped by default in every CI job.
/// Run explicitly with --gtest_also_run_disabled_tests.
TEST(SnapshotZstdBench, DISABLED_V2vsV3)
{
    struct Case { int nodes; int value_bytes; const char * label; };
    std::vector<Case> cases = {
        {100000, 128,  "100K x 128B"},
        {100000, 307,  "100K x 0.3KB"},
        {50000,  1024, "50K x 1KB"},
    };

    fprintf(stderr, "\n%-8s %10s %11s %10s %10s %8s\n",
        "version", "nodes", "create_ms", "load_ms", "disk_MB", "ratio");
    fprintf(stderr, "%s\n", std::string(60, '-').c_str());

    int idx = 0;
    for (auto & c : cases)
    {
        auto v2 = runOne(SnapshotVersion::V2, c.nodes, c.value_bytes, idx++);
        auto v3 = runOne(SnapshotVersion::V3, c.nodes, c.value_bytes, idx++);
        v3.ratio = v3.disk_bytes > 0 ? static_cast<double>(v2.disk_bytes) / v3.disk_bytes : 1.0;

        fprintf(stderr, "── %s ──\n", c.label);
        fprintf(stderr, "%-8s %10d %11.1f %10.1f %10.2f %8s\n",
            "V2", v2.nodes, v2.create_ms, v2.load_ms, v2.disk_bytes / 1e6, "1.00");
        fprintf(stderr, "%-8s %10d %11.1f %10.1f %10.2f %8.2f\n",
            "V3", v3.nodes, v3.create_ms, v3.load_ms, v3.disk_bytes / 1e6, v3.ratio);
    }

    SUCCEED();
}
