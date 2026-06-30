import time

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients

cluster = RaftKeeperCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_service_keeper1.xml'],
                             stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_service_keeper2.xml'],
                             stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_service_keeper3.xml'],
                             stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_snapshot_corruption_fallback(started_cluster):
    """Corrupt latest snapshot's object-1 (IntMap), verify node recovers from older snapshot."""
    zk1 = None
    try:
        zk1 = node1.get_fake_zk()

        # Write enough entries to create multiple snapshots (snapshot_distance=30).
        # Each create is one Raft log entry. 120 creates ≈ 4 snapshots.
        for i in range(120):
            zk1.create(f"/test_corrupt_snap_{i}", b"data")

        # Ensure they're committed everywhere
        zk1.sync("/")
        time.sleep(3)

        # Verify multiple snapshots exist
        snap_files = node1.exec_in_container(
            ['bash', '-c',
             'ls /var/lib/raftkeeper/data/raft_snapshot/snapshot_*_1 2>/dev/null | wc -l'])
        snap_count = int(snap_files.strip())
        assert snap_count >= 2, f"Expected >=2 snapshots (object-1 files), got {snap_count}"

        # Stop node1 gracefully to get a clean filesystem state
        node1.stop_raftkeeper()

        # Explicitly corrupt the latest snapshot's object 1 (IntMap) by overwriting
        # it with random bytes. Object 1 is required for snapshot loading
        # (it carries zxid, session_id_counter, OBJECTCOUNT) — corrupting it
        # guarantees the latest snapshot fails to deserialize.
        result = node1.exec_in_container(
            ['bash', '-c',
             'LATEST=$(ls -t /var/lib/raftkeeper/data/raft_snapshot/snapshot_*_1 | head -1); '
             'if [ -z "$LATEST" ]; then echo "ERROR: no snapshot object 1 found" >&2; exit 1; fi; '
             'echo "corrupting latest snapshot: $LATEST"; '
             'SIZE=$(stat -c%s "$LATEST"); '
             'dd if=/dev/urandom of="$LATEST" bs=1 count="$SIZE" conv=notrunc status=none'])
        print(f"Corruption result: {result}")

        # Start node1 — should fallback to an older snapshot
        node1.start_raftkeeper()
        node1.wait_for_join_cluster()

        # Verify all data is present (recovered from older snapshot + log replay)
        zk1_new = node1.get_fake_zk()
        for i in range(120):
            assert zk1_new.exists(f"/test_corrupt_snap_{i}") is not None, \
                f"Missing node /test_corrupt_snap_{i} after recovery"

        close_zk_clients([zk1_new])

    finally:
        close_zk_clients([zk1])


def test_log_tail_corruption_recovery(started_cluster):
    """Append garbage bytes to the open log segment, verify the node recovers on restart."""
    zk1 = zk2 = None
    try:
        zk1 = node1.get_fake_zk()
        zk2 = node2.get_fake_zk()

        # Write some data so the open log segment has real entries
        for i in range(50):
            zk1.create(f"/test_log_tail_{i}", b"data")

        zk2.sync("/")
        time.sleep(1)

        # Stop node1 gracefully so the file system is in a clean state
        node1.stop_raftkeeper()

        # Explicitly corrupt the open log segment by appending random bytes —
        # this simulates a crash mid-write that left a partial log entry.
        # Using kill=True is unreliable: fsync + write atomicity often produce
        # clean files even on SIGKILL.
        result = node1.exec_in_container(
            ['bash', '-c',
             'OPEN=$(ls /var/lib/raftkeeper/data/raft_log/log_*_open_* 2>/dev/null | head -1); '
             'if [ -z "$OPEN" ]; then echo "ERROR: no open segment found" >&2; exit 1; fi; '
             'echo "corrupting open segment: $OPEN"; '
             'dd if=/dev/urandom of="$OPEN" bs=1 count=20 oflag=append conv=notrunc status=none'])
        print(f"Corruption result: {result}")

        # Start node1 — should detect incomplete tail entry, truncate it, and recover
        node1.start_raftkeeper()
        node1.wait_for_join_cluster()

        # Verify all data is intact
        zk1_new = node1.get_fake_zk()
        for i in range(50):
            assert zk1_new.exists(f"/test_log_tail_{i}") is not None, \
                f"Missing node /test_log_tail_{i} after recovery"

        # Can still write new data
        zk1_new.create("/test_log_tail_recovered", b"ok")
        assert zk1_new.get("/test_log_tail_recovered")[0] == b"ok"

        close_zk_clients([zk1_new])

    finally:
        close_zk_clients([zk1, zk2])
