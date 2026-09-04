"""Back-to-back behavioral comparison: RaftKeeper vs a real ClickHouse Keeper.

This mirrors test_back_to_back (which compares RaftKeeper against Apache ZooKeeper), but the
"genuine" side here is a real ClickHouse Keeper container (see
helpers/docker_compose_clickhouse_keeper.yml). It focuses on the ClickHouse-specific extension
ops (OpNum 500-507) and the behaviors aligned in docs/keeper-compatibility-audit.md.

IMPORTANT: parity with ClickHouse Keeper is only expected from the RaftKeeper binary built in
ClickHouse-compat mode (`bash build.sh clickhouse`, i.e. COMPATIBLE_MODE_ZOOKEEPER=OFF). In the
default ZooKeeper-compat build RaftKeeper deliberately matches Apache ZooKeeper, which diverges
from ClickHouse Keeper (cversion accounting, parent-cversion-on-Set, ...). Point
RAFTKEEPER_TESTS_SERVER_BIN_PATH at the clickhouse-mode binary when running this suite.

Absolute stat values (czxid/zxid/ctime) legitimately differ between two independent servers, so
these tests compare *structure* (sorted names) and *behavioral parity* (same success/error
outcome), not raw stat numbers.
"""
import pytest

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import KeeperFeatureClient, close_zk_clients

cluster = RaftKeeperCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper_single_node.xml'],
                             with_clickhouse_keeper=True, stay_alive=True)


def get_raftkeeper():
    zk = KeeperFeatureClient(hosts=cluster.get_instance_ip("node1") + ":8101", timeout=60.0)
    zk.start()
    return zk


def get_clickhouse_keeper():
    return cluster.get_clickhouse_keeper_client()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        try:
            cluster.start()
        except Exception as ex:
            # This suite needs a real ClickHouse Keeper container. If the environment can't provide
            # its image (e.g. no registry egress from the docker-in-docker daemon), skip rather than
            # fail the whole integration matrix - RaftKeeper's own startup is covered by other tests.
            cluster.shutdown()
            pytest.skip(f"ClickHouse Keeper unavailable, skipping back-to-back suite: {ex}")
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
