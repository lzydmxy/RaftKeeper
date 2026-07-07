#!/usr/bin/env python3
"""Integration test for zstd-compressed snapshots (format V3).

Verifies that:
1. A node with snapshot_compression=zstd writes V3 snapshot objects on disk.
2. State restored purely from a zstd snapshot (logs wiped) is intact.
3. Data survives a hard restart (kill -9) that reloads from a zstd snapshot.
"""
import time

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients

cluster = RaftKeeperCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
)

SNAP_DIR = "/var/lib/raftkeeper/data/raft_snapshot"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def snapshot_object_files(n):
    """Data snapshot object file names (skip the 4 meta objects _1.._4)."""
    out = n.exec_in_container(["bash", "-c", f"ls {SNAP_DIR} 2>/dev/null || true"])
    return [f for f in out.split() if f.startswith("snapshot_")]


def is_v3(n, fname):
    """Header is 8-byte 'SnapHead' magic + 1 version byte; V3 == 0x03."""
    byte = n.exec_in_container(
        ["bash", "-c", f"dd if={SNAP_DIR}/{fname} bs=1 skip=8 count=1 2>/dev/null | od -An -tu1 | tr -d ' \\n'"]
    ).strip()
    return byte == "3"


def force_snapshot(n):
    n.send_4lw_cmd("csnp")


def test_snapshot_written_as_v3(started_cluster):
    """Snapshot objects on disk must carry the V3 version byte."""
    zk = None
    try:
        zk = node.get_fake_zk(session_timeout=120)
        for i in range(40):  # exceed snapshot_distance=20
            zk.create(f"/test_v3_{i}", f"data{i}".encode())
        close_zk_clients([zk])
        zk = None

        force_snapshot(node)

        # csnp schedules the snapshot; poll for the object to land on disk
        files = []
        for _ in range(30):
            files = snapshot_object_files(node)
            if files and any(is_v3(node, f) for f in files):
                break
            time.sleep(1)
        assert files, "no snapshot objects were created"
        assert any(is_v3(node, f) for f in files), f"no V3 snapshot object among {files}"
    finally:
        close_zk_clients([zk])


def test_restore_purely_from_zstd_snapshot(started_cluster):
    """Wipe logs, keep only the zstd snapshot, and confirm state reloads."""
    zk = zk2 = None
    try:
        zk = node.get_fake_zk(session_timeout=120)
        zk.create("/test_v3_restore", b"root")
        for i in range(40):
            zk.create(f"/test_v3_restore/n{i}", f"v{i}".encode())
        close_zk_clients([zk])
        zk = None

        force_snapshot(node)

        node.stop_raftkeeper()
        # remove logs so recovery must come from the zstd snapshot alone
        node.exec_in_container(["bash", "-c", "rm -fr /var/lib/raftkeeper/data/raft_log/*"])
        node.start_raftkeeper(start_wait=True)
        node.wait_for_join_cluster()

        zk2 = node.get_fake_zk()
        assert zk2.get("/test_v3_restore")[0] == b"root"
        for i in range(40):
            assert zk2.get(f"/test_v3_restore/n{i}")[0] == f"v{i}".encode()
    finally:
        close_zk_clients([zk, zk2])


def test_zstd_snapshot_survives_hard_restart(started_cluster):
    """Data written under zstd snapshots must survive a kill -9 restart."""
    zk = zk2 = None
    try:
        zk = node.get_fake_zk(session_timeout=120)
        zk.create("/test_v3_hard", b"root")
        for i in range(40):
            zk.create(f"/test_v3_hard/n{i}", str(i).encode())
        close_zk_clients([zk])
        zk = None

        force_snapshot(node)

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        zk2 = node.get_fake_zk()
        assert zk2.get("/test_v3_hard")[0] == b"root"
        for i in range(40):
            assert zk2.get(f"/test_v3_hard/n{i}")[0] == str(i).encode()
    finally:
        close_zk_clients([zk, zk2])
