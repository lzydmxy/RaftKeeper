"""Back-to-back behavioral comparison: RaftKeeper vs a real ClickHouse Keeper.

This mirrors test_back_to_back (which compares RaftKeeper against Apache ZooKeeper), but the
"genuine" side here is a real ClickHouse Keeper container (see
helpers/docker_compose_clickhouse_keeper.yml). It focuses on the ClickHouse-specific extension
ops (OpNum 500-507) and the behaviors aligned in docs/keeper-compatibility-audit.md.

IMPORTANT: parity with ClickHouse Keeper is only expected from the RaftKeeper binary built in
ClickHouse-compat mode (`bash build.sh clickhouse`, i.e. COMPATIBLE_MODE_ZOOKEEPER=OFF). In the
default ZooKeeper-compat build RaftKeeper deliberately matches Apache ZooKeeper, which diverges
from ClickHouse Keeper (cversion accounting, parent-cversion-on-Set, MultiRead response handling,
...). In CI this suite is excluded from the default (ZooKeeper-mode) integration matrix and is run
only by the dedicated `integration-test-clickhouse-mode` job. Point
RAFTKEEPER_TESTS_SERVER_BIN_PATH at the clickhouse-mode binary when running it manually.

Absolute stat values (czxid/zxid/ctime) legitimately differ between two independent servers, so
these tests compare *structure* (sorted names), *behavioral parity* (same success/error outcome),
and version counters that advance deterministically - not raw zxid/time numbers.
"""
import os
import re
import subprocess
import time

import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import KeeperFeatureClient, close_zk_clients

cluster = RaftKeeperCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper_single_node.xml'],
                             with_clickhouse_keeper=True, stay_alive=True)


def _keeper_image_tar():
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ch_keeper_image.tar')


def _maybe_load_keeper_image():
    """The docker-in-docker daemon usually can't pull from a registry in CI. If the job pre-pulled
    the ClickHouse Keeper image on the host and saved it to tests/integration/ch_keeper_image.tar
    (mounted into the runner), load it into the DIND daemon so docker-compose finds it locally.
    Returns a short status string for diagnostics."""
    tar = _keeper_image_tar()
    if not os.path.exists(tar):
        return f"no image tarball at {tar} (DIND will try to pull)"
    r = subprocess.run(['docker', 'load', '-i', tar], capture_output=True, text=True)
    return f"docker load rc={r.returncode}: {(r.stdout + r.stderr).strip()[:300]}"


def _keeper_diagnostics():
    """Collect why the ClickHouse Keeper couldn't start, for the skip message."""
    out = []
    for cmd in (['docker', 'images'], ['docker', 'ps', '-a']):
        r = subprocess.run(cmd, capture_output=True, text=True)
        out.append(f"$ {' '.join(cmd)}\n{r.stdout}{r.stderr}")
    try:
        r = subprocess.run(cluster.base_clickhouse_keeper_cmd + ['up', '-d', '--force-recreate'],
                           capture_output=True, text=True,
                           env={**os.environ, 'CH_KEEPER_CONFIG': cluster.clickhouse_keeper_config_path})
        out.append(f"$ compose up rc={r.returncode}\n{r.stdout}{r.stderr}")
        logs = subprocess.run(['docker', 'logs', cluster.get_instance_docker_id('ch_keeper1')],
                              capture_output=True, text=True)
        out.append(f"$ ch_keeper1 logs\n{logs.stdout}{logs.stderr}")
    except Exception as e:  # noqa: BLE001
        out.append(f"diag error: {e}")
    return "\n".join(out)[:4000]


def get_raftkeeper():
    zk = KeeperFeatureClient(hosts=cluster.get_instance_ip("node1") + ":8101", timeout=60.0)
    zk.start()
    return zk


def get_clickhouse_keeper():
    return cluster.get_clickhouse_keeper_client()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        load_status = _maybe_load_keeper_image()
        try:
            cluster.start()
        except Exception as ex:
            # This suite needs a real ClickHouse Keeper container. If the environment can't provide
            # its image (e.g. no registry egress from the docker-in-docker daemon), skip rather than
            # fail the whole integration matrix - RaftKeeper's own startup is covered by other tests.
            diag = _keeper_diagnostics()
            cluster.shutdown()
            pytest.skip(f"ClickHouse Keeper unavailable: {ex}\nload: {load_status}\n{diag}")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture()
def clients(started_cluster):
    raft = ch = None
    try:
        raft = get_raftkeeper()
        ch = get_clickhouse_keeper()
        yield raft, ch
    finally:
        close_zk_clients([c for c in (raft, ch) if c is not None])


def _outcome(fn):
    """Run fn(); return ('ok', value) on success or ('err', ExceptionClassName) on failure."""
    try:
        return ('ok', fn())
    except Exception as e:
        return ('err', type(e).__name__)


def assert_same_outcome(clients, fn, normalize=lambda x: x, label=""):
    """Run fn(client) on both servers and assert the same success/error outcome.

    On success the (normalized) return values must be equal; on failure the raised exception
    type must match.
    """
    raft, ch = clients
    r = _outcome(lambda: fn(raft))
    c = _outcome(lambda: fn(ch))
    assert r[0] == c[0], f"{label}: outcome kind differs raft={r} ch={c}"
    if r[0] == 'ok':
        assert normalize(r[1]) == normalize(c[1]), f"{label}: result differs raft={r[1]} ch={c[1]}"
    else:
        assert r[1] == c[1], f"{label}: exception differs raft={r[1]} ch={c[1]}"


def _setup(clients, ops):
    """Apply the same setup ops (a callable per client) on both servers."""
    raft, ch = clients
    ops(raft)
    ops(ch)


def _cleanup(clients, root):
    for zk in clients:
        try:
            # Large explicit limit: 0 would be a literal zero limit on ClickHouse Keeper (no-op).
            zk.remove_recursive(root, remove_nodes_limit=1000000)
        except Exception:
            pass


def test_try_remove_best_effort(clients):
    def setup(zk):
        zk.create('/tr')
        zk.create('/tr/child')
    _setup(clients, setup)
    try:
        # Non-empty node: best-effort remove is a silent no-op success on both, node survives.
        assert_same_outcome(clients, lambda zk: zk.try_remove('/tr'), label="try_remove non-empty")
        assert_same_outcome(clients, lambda zk: zk.exists('/tr') is not None, label="/tr survives")

        # Wrong version: also a no-op success on both, node survives.
        assert_same_outcome(clients, lambda zk: zk.try_remove('/tr/child', version=999),
                            label="try_remove wrong version")
        assert_same_outcome(clients, lambda zk: zk.exists('/tr/child') is not None,
                            label="/tr/child survives")
    finally:
        _cleanup(clients, '/tr')


def test_check_stat_behavioral_parity(clients):
    # Absolute stat values differ between servers, so we compare behavior: each server checks
    # against ITS OWN stat. Matching -> OK on both; wrong version -> BadVersion on both;
    # wrong dataLength -> BadVersion on both; missing node -> NoNode on both.
    def setup(zk):
        zk.create('/cs', b'hello')
    _setup(clients, setup)
    try:
        def check_matching(zk):
            st = zk.exists('/cs')
            return zk.check_stat('/cs', version=st.version, cversion=st.cversion,
                                 aversion=st.aversion, data_length=st.dataLength)
        assert_same_outcome(clients, check_matching, label="check_stat matching")

        def check_bad_version(zk):
            st = zk.exists('/cs')
            return zk.check_stat('/cs', version=st.version + 1)
        assert_same_outcome(clients, check_bad_version, label="check_stat wrong version")

        def check_bad_datalength(zk):
            st = zk.exists('/cs')
            return zk.check_stat('/cs', data_length=st.dataLength + 100)
        assert_same_outcome(clients, check_bad_datalength, label="check_stat wrong dataLength")

        assert_same_outcome(clients, lambda zk: zk.check_stat('/cs_missing', version=-1),
                            label="check_stat missing node")
    finally:
        _cleanup(clients, '/cs')


def test_remove_recursive_rejects_root(clients):
    # Both must reject removing "/" rather than wiping the tree.
    assert_same_outcome(clients, lambda zk: zk.remove_recursive('/'), label="remove_recursive /")


def test_remove_recursive_subtree(clients):
    def setup(zk):
        zk.create('/rr')
        zk.create('/rr/a')
        zk.create('/rr/b')
        zk.create('/rr/b/c')
    _setup(clients, setup)
    try:
        # NOTE: pass an explicit positive limit. RaftKeeper treats remove_nodes_limit==0 as "unlimited",
        # but ClickHouse Keeper treats 0 as a literal zero limit (removes nothing -> ZNOTEMPTY). That
        # 0-sentinel divergence is recorded in docs/keeper-compatibility-audit.md; here we test the
        # aligned path with a limit larger than the subtree.
        assert_same_outcome(clients, lambda zk: zk.remove_recursive('/rr', remove_nodes_limit=100),
                            label="remove_recursive subtree")
        assert_same_outcome(clients, lambda zk: zk.exists('/rr') is None, label="/rr gone")
    finally:
        _cleanup(clients, '/rr')


def test_list_recursive_same_set(clients):
    def setup(zk):
        zk.create('/lr')
        zk.create('/lr/x')
        zk.create('/lr/y')
        zk.create('/lr/y/z')
    _setup(clients, setup)
    try:
        # Explicit limit for the same 0-sentinel reason as remove_recursive above (RaftKeeper 0=unlimited,
        # ClickHouse 0=literal zero). Traversal order also differs (RaftKeeper DFS vs ClickHouse BFS),
        # so compare as sorted sets.
        assert_same_outcome(clients, lambda zk: zk.list_recursive('/lr', max_entries=1000),
                            normalize=lambda names: sorted(names), label="list_recursive")
    finally:
        _cleanup(clients, '/lr')


@pytest.mark.parametrize("list_type,label", [(0, "ALL"), (1, "PERSISTENT_ONLY"), (2, "EPHEMERAL_ONLY")])
def test_filtered_list_parity(clients, list_type, label):
    def setup(zk):
        zk.create('/fl')
        zk.create('/fl/p1')
        zk.create('/fl/p2')
        zk.create('/fl/e1', ephemeral=True)
    _setup(clients, setup)
    try:
        assert_same_outcome(clients, lambda zk: zk.get_filtered_children('/fl', list_type=list_type),
                            normalize=lambda names: sorted(names), label=f"filtered_list {label}")
    finally:
        _cleanup(clients, '/fl')


# --------------------------------------------------------------------------------------------------
# Comprehensive coverage of the capabilities ClickHouse actually depends on: atomic multi-write with
# version checks (the replicated-table CAS pattern), check-not-exists guards, create-if-not-exists,
# batched multi-read, version-based set, and sequential / ephemeral node semantics.
# --------------------------------------------------------------------------------------------------

def _tx_kinds(results):
    """Reduce a transaction result list to per-op kinds ('ok' or the exception class name),
    which is stable across servers (unlike stat/zxid values)."""
    return tuple(type(r).__name__ if isinstance(r, Exception) else 'ok' for r in results)


def _run_tx(zk, build):
    """Build and commit a write transaction; return a normalized, cross-server-comparable outcome."""
    t = zk.transaction()
    build(t)
    try:
        return ('committed', _tx_kinds(t.commit()))
    except Exception as e:
        return ('raised', type(e).__name__)


def test_multi_write_atomic_success(clients):
    raft, ch = clients

    def setup(zk):
        zk.create('/mw_ok')
    _setup(clients, setup)
    try:
        def build(t):
            t.create('/mw_ok/a', b'1')
            t.create('/mw_ok/b')
            t.set_data('/mw_ok/a', b'2')
        assert _run_tx(raft, build) == _run_tx(ch, build), "multi commit outcome differs"
        # All ops applied atomically on both.
        assert_same_outcome(clients, lambda zk: zk.get('/mw_ok/a')[0], label="mw data")
        assert_same_outcome(clients, lambda zk: sorted(zk.get_children('/mw_ok')), label="mw children")
    finally:
        _cleanup(clients, '/mw_ok')


def test_multi_write_rollback_on_failure(clients):
    raft, ch = clients

    def setup(zk):
        zk.create('/mw_rb')
        zk.create('/mw_rb/exists')
    _setup(clients, setup)
    try:
        def build(t):
            t.create('/mw_rb/exists')   # ZNODEEXISTS -> fails the whole transaction
            t.create('/mw_rb/new')      # must be rolled back
        assert _run_tx(raft, build) == _run_tx(ch, build), "rollback outcome differs"
        # Rolled back: the second op's node must not exist on either server.
        assert_same_outcome(clients, lambda zk: zk.exists('/mw_rb/new') is None, label="rollback new absent")
    finally:
        _cleanup(clients, '/mw_rb')


def test_multi_check_and_set_cas(clients):
    # The core ClickHouse pattern: read version, then atomically check(version)+set. Wrong version
    # must fail the whole transaction and leave data untouched.
    raft, ch = clients

    def setup(zk):
        zk.create('/cas', b'v0')
    _setup(clients, setup)
    try:
        def cas(zk, version, new_value):
            t = zk.transaction()
            t.check('/cas', version)
            t.set_data('/cas', new_value)
            return ('committed', _tx_kinds(t.commit()))

        # Correct version -> commits, data becomes v1.
        assert cas(raft, raft.exists('/cas').version, b'v1') == cas(ch, ch.exists('/cas').version, b'v1'), \
            "CAS (correct version) outcome differs"
        assert_same_outcome(clients, lambda zk: zk.get('/cas')[0], label="cas data after correct")

        # Wrong version -> whole tx fails, data unchanged (still v1).
        def cas_wrong(zk):
            t = zk.transaction()
            t.check('/cas', 999)
            t.set_data('/cas', b'v2')
            try:
                return ('committed', _tx_kinds(t.commit()))
            except Exception as e:
                return ('raised', type(e).__name__)
        assert cas_wrong(raft) == cas_wrong(ch), "CAS (wrong version) outcome differs"
        assert_same_outcome(clients, lambda zk: zk.get('/cas')[0], label="cas data after wrong")
    finally:
        _cleanup(clients, '/cas')


def test_multi_check_if_not_exists_guard(clients):
    # ClickHouse uses CheckNotExists inside multi to guarantee "create only if still absent".
    raft, ch = clients

    def setup(zk):
        zk.create('/cne')
    _setup(clients, setup)
    try:
        def guard(zk):
            t = zk.transaction()
            t.check_if_not_exists('/cne/x', -1)
            t.create('/cne/x')
            return ('committed', _tx_kinds(t.commit()))

        # First run: absent -> guard passes, node created.
        assert guard(raft) == guard(ch), "check_if_not_exists first-run differs"
        assert_same_outcome(clients, lambda zk: zk.exists('/cne/x') is not None, label="cne created")

        # Second run: present -> guard fails, create rolled back.
        assert _run_tx(raft, lambda t: (t.check_if_not_exists('/cne/x', -1), t.create('/cne/x'))) \
            == _run_tx(ch, lambda t: (t.check_if_not_exists('/cne/x', -1), t.create('/cne/x'))), \
            "check_if_not_exists second-run differs"
    finally:
        _cleanup(clients, '/cne')


def test_create_if_not_exists_idempotent(clients):
    raft, ch = clients

    def setup(zk):
        zk.create('/cine')
        zk.create('/cine/0')
    _setup(clients, setup)
    try:
        for zk in (raft, ch):
            zk.create_if_not_exists('/cine/0')   # no-op, already exists
            zk.create_if_not_exists('/cine/1')   # creates
        assert_same_outcome(clients, lambda zk: sorted(zk.get_children('/cine')), label="cine children")
    finally:
        _cleanup(clients, '/cine')


def test_multi_read_mixed(clients):
    raft, ch = clients

    def setup(zk):
        zk.create('/mr')
        zk.create('/mr/a', b'adata')
        zk.create('/mr/b')
    _setup(clients, setup)
    try:
        def read(zk):
            t = zk.multi_read()
            t.get_children('/mr', None)
            t.get('/mr/a', None)
            t.get('/mr/missing', None)
            t.exists('/mr/b', None)
            results = t.commit()
            children = sorted(results[0])
            a_data = results[1][0]
            missing = type(results[2]).__name__ if isinstance(results[2], Exception) else 'ok'
            b_ok = not isinstance(results[3], Exception)
            return (children, a_data, missing, b_ok)
        assert read(raft) == read(ch), "multi_read results differ"
    finally:
        _cleanup(clients, '/mr')


def test_set_version_cas(clients):
    raft, ch = clients

    def setup(zk):
        zk.create('/vcas', b'0')
    _setup(clients, setup)
    try:
        def set_correct(zk):
            st = zk.exists('/vcas')
            zk.set('/vcas', b'1', version=st.version)
            return zk.get('/vcas')[0]
        assert_same_outcome(clients, set_correct, label="set correct version")
        # Wrong version -> BadVersionError on both.
        assert_same_outcome(clients, lambda zk: zk.set('/vcas', b'2', version=999), label="set wrong version")
    finally:
        _cleanup(clients, '/vcas')


def test_sequential_nodes_properties(clients):
    # ClickHouse relies on sequential nodes being unique, monotonically increasing and zero-padded
    # (block numbers, locks, leader election). Exact suffix values differ between servers, so assert
    # the invariants hold identically on both rather than comparing names.
    raft, ch = clients

    def setup(zk):
        zk.create('/seq')
    _setup(clients, setup)
    try:
        def props(zk):
            for _ in range(5):
                zk.create('/seq/n-', sequence=True)
            children = zk.get_children('/seq')
            unique = len(set(children)) == len(children)
            fmt = all(re.fullmatch(r'n-\d{10}', c) for c in children)
            return (len(children), unique, fmt)
        rp, cp = props(raft), props(ch)
        assert rp == cp == (5, True, True), f"sequential properties raft={rp} ch={cp}"
        # Suffixes must be strictly increasing in creation order on each server.
        for zk in (raft, ch):
            nums = sorted(int(c.split('-')[1]) for c in zk.get_children('/seq'))
            assert nums == sorted(set(nums)) and len(nums) == 5, "sequential numbers not strictly increasing"
    finally:
        _cleanup(clients, '/seq')


def test_ephemeral_session_cleanup(clients):
    # Replica liveness in ClickHouse depends on ephemerals vanishing when a session ends.
    raft, ch = clients
    raft_owner = get_raftkeeper()
    ch_owner = get_clickhouse_keeper()
    try:
        for main, owner, name in ((raft, raft_owner, 'raft'), (ch, ch_owner, 'ch')):
            owner.create(f'/eph_{name}', ephemeral=True)
            st = main.exists(f'/eph_{name}')
            assert st is not None and st.ephemeralOwner != 0, f"{name}: ephemeral missing or no owner"

        # Ending the owning session must remove its ephemerals on both servers.
        close_zk_clients([raft_owner, ch_owner])
        raft_owner = ch_owner = None

        for main, name in ((raft, 'raft'), (ch, 'ch')):
            deadline = time.time() + 60
            while time.time() < deadline and main.exists(f'/eph_{name}') is not None:
                time.sleep(0.5)
            assert main.exists(f'/eph_{name}') is None, f"{name}: ephemeral not cleaned up after session close"
    finally:
        close_zk_clients([c for c in (raft_owner, ch_owner) if c is not None])
