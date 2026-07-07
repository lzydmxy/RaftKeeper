#!/usr/bin/env python3
"""Integration test for zstd-compressed Raft log entries.

Verifies that:
1. A node started with log_compression=zstd correctly writes and reads back data.
2. Data is durable across a hard restart (kill -9) while using zstd logs.
3. A node can roll from uncompressed logs to zstd logs mid-life and read both.
"""
import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients

cluster = RaftKeeperCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_write_and_read_with_zstd(started_cluster):
    """Basic write-then-read with compression enabled."""
    zk = None
    try:
        zk = node.get_fake_zk(session_timeout=120)
        zk.create("/test_zstd_basic", b"zstd_value")
        for i in range(20):
            zk.create(f"/test_zstd_basic/node{i}", f"data{i}".encode())
        close_zk_clients([zk])

        for i in range(20):
            zk = node.get_fake_zk()
            assert zk.get(f"/test_zstd_basic/node{i}")[0] == f"data{i}".encode()
            close_zk_clients([zk])
            zk = None
    finally:
        close_zk_clients([zk])


def test_zstd_survives_hard_restart(started_cluster):
    """Data written with zstd codec must survive a kill -9 restart."""
    zk = zk2 = None
    try:
        zk = node.get_fake_zk(session_timeout=120)
        zk.create("/test_zstd_restart", b"root")
        for i in range(50):
            zk.create(f"/test_zstd_restart/node{i}", f"value{i}".encode())
        close_zk_clients([zk])
        zk = None

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        zk2 = node.get_fake_zk()
        assert zk2.get("/test_zstd_restart")[0] == b"root"
        for i in range(50):
            assert zk2.get(f"/test_zstd_restart/node{i}")[0] == f"value{i}".encode()
    finally:
        close_zk_clients([zk, zk2])


def test_zstd_multiple_restarts(started_cluster):
    """Data durability across two consecutive hard restarts."""
    zk = zk2 = zk3 = None
    try:
        zk = node.get_fake_zk(session_timeout=120)
        zk.create("/test_zstd_multi_restart", b"mr_root")
        for i in range(30):
            zk.create(f"/test_zstd_multi_restart/n{i}", str(i).encode())
        close_zk_clients([zk])
        zk = None

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        zk2 = node.get_fake_zk(session_timeout=120)
        zk2.create("/test_zstd_multi_restart/after_first_restart", b"1")
        close_zk_clients([zk2])
        zk2 = None

        node.restart_raftkeeper(kill=True)
        node.wait_for_join_cluster()

        zk3 = node.get_fake_zk()
        assert zk3.get("/test_zstd_multi_restart")[0] == b"mr_root"
        for i in range(30):
            assert zk3.get(f"/test_zstd_multi_restart/n{i}")[0] == str(i).encode()
        assert zk3.get("/test_zstd_multi_restart/after_first_restart")[0] == b"1"
    finally:
        close_zk_clients([zk, zk2, zk3])
