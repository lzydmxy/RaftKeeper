import os
import random

import pytest
from kazoo.client import KazooClient
from multiprocessing.dummy import Pool

from helpers.cluster_service import RaftKeeperCluster
from helpers.utils import close_zk_clients

cluster1 = RaftKeeperCluster(__file__)
node1 = cluster1.add_instance('node1', main_configs=['configs/enable_service_keeper1.xml', 'configs/log_conf.xml'],
                              stay_alive=True)
node2 = cluster1.add_instance('node2', main_configs=['configs/enable_service_keeper2.xml', 'configs/log_conf.xml'],
                              stay_alive=True)
node3 = cluster1.add_instance('node3', main_configs=['configs/enable_service_keeper3.xml', 'configs/log_conf.xml'],
                              stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster1.start()

        yield cluster1

    finally:
        cluster1.shutdown()


def smaller_exception(ex):
    return '\n'.join(str(ex).split('\n')[0:2])


def wait_node(node):
    node.wait_for_join_cluster()


def wait_nodes():
    for node in [node1, node2, node3]:
        wait_node(node)


def get_fake_zk(node, timeout=60.0):
    _fake_zk_instance = KazooClient(hosts=cluster1.get_instance_ip(node.name) + ":8101", timeout=timeout)
    _fake_zk_instance.start()
    return _fake_zk_instance


def compare_stats(stat1, stat2, path):
    assert stat1.czxid == stat2.czxid, "path " + path + " cxzids not equal for stats: " + str(
        stat1.czxid) + " != " + str(stat2.czxid)
    assert stat1.mzxid == stat2.mzxid, "path " + path + " mxzids not equal for stats: " + str(
        stat1.mzxid) + " != " + str(stat2.mzxid)
    assert stat1.version == stat2.version, "path " + path + " versions not equal for stats: " + str(
        stat1.version) + " != " + str(stat2.version)
    assert stat1.cversion == stat2.cversion, "path " + path + " cversions not equal for stats: " + str(
        stat1.cversion) + " != " + str(stat2.cversion)
    # assert stat1.aversion == stat2.aversion, "path " + path + " aversions not equal for stats: " + str(
    # stat1.aversion) + " != " + str(stat2.aversion)  ACL
    assert stat1.ephemeralOwner == stat2.ephemeralOwner, "path " + path + " ephemeralOwners not equal for stats: " + str(
        stat1.ephemeralOwner) + " != " + str(stat2.ephemeralOwner)
    assert stat1.dataLength == stat2.dataLength, "path " + path + " ephemeralOwners not equal for stats: " + str(
        stat1.dataLength) + " != " + str(stat2.dataLength)
    assert stat1.numChildren == stat2.numChildren, "path " + path + " numChildren not equal for stats: " + str(
        stat1.numChildren) + " != " + str(stat2.numChildren)
    # assert stat1.pzxid == stat2.pzxid, "path " + path + " pzxid not equal for stats: " + str(stat1.pzxid) + " != "
    # + str(stat2.pzxid) from fuzzy snapshot


def dump_states(zk1, d, path="/"):
    data1, stat1 = zk1.get(path)

    d[path] = (data1, stat1)

    first_children = list(sorted(zk1.get_children(path)))

    for children in first_children:
        dump_states(zk1, d, os.path.join(path, children))


def start_raftkeeper(node):
    node.start_raftkeeper(60)
    node.wait_for_join_cluster()


def set_async_snapshot_true():
    for index, node in [(1, node1), (2, node2), (3, node3)]:
        node.stop_raftkeeper()
        node.replace_in_config(f'/etc/raftkeeper-server/config.d/enable_service_keeper{index}.xml', '<async_snapshot>false', '<async_snapshot>true')
        node.exec_in_container(
            ['bash', '-c', 'rm -fr /var/lib/raftkeeper/data/raft_log/* /var/lib/raftkeeper/data/raft_snapshot/*'])

    p = Pool(3)
    result = p.map_async(start_raftkeeper, [node1, node2, node3])
    result.wait()


@pytest.mark.parametrize('async_snapshot', [False, True])
def test_restart(started_cluster, async_snapshot):
    if async_snapshot:
        set_async_snapshot_true()
    fake_zks = [get_fake_zk(node) for node in [node1, node2, node3]]
    try:
        fake_zks[0].create("/test_restart_node", b"hello")

        for i in range(10):
            fake_zk = random.choice(fake_zks)
            fake_zk.create("/test_restart_node/" + str(i), b"hello")

        fake_zks[1].create("/test_restart_node1", b"hello")

        for i in range(10):
            fake_zk = random.choice(fake_zks)
            fake_zk.create("/test_restart_node1/" + str(i), b"hello")

        fake_zks[2].create("/test_restart_node2", b"hello")

        for i in range(10):
            fake_zk = random.choice(fake_zks)
            t = fake_zk.transaction()
            t.create("/test_restart_node2/q" + str(i))
            # delete not exist node
            t.delete("/test_restart_node2/a" + str(i))
            t.create("/test_restart_node2/x" + str(i))
            t.commit()

        fake_zk = random.choice(fake_zks)
        d = {}
        dump_states(fake_zk, d)

        close_zk_clients(fake_zks)

        node1.stop_raftkeeper()
        node2.stop_raftkeeper()
        node3.stop_raftkeeper()

        node1.start_raftkeeper(start_wait=False)
        node2.start_raftkeeper(start_wait=False)
        node3.start_raftkeeper(start_wait=False)

        wait_nodes()

        fake_zks = [get_fake_zk(node) for node in [node1, node2, node3]]

        for i in range(10):
            fake_zk = random.choice(fake_zks)
            assert fake_zk.get("/test_restart_node/" + str(i))[0] == b"hello"

        for i in range(10):
            fake_zk = random.choice(fake_zks)
            assert fake_zk.get("/test_restart_node1/" + str(i))[0] == b"hello"

        fake_zk = random.choice(fake_zks)
        children = fake_zk.get_children("/test_restart_node2")

        assert children == []

        dd = {}
        dump_states(fake_zk, dd)

        fake_zk = random.choice(fake_zks)

        ddd = {}
        dump_states(fake_zk, ddd)

        assert len(d) == len(dd)
        assert len(d) == len(ddd)
        for k, v in d.items():
            if k not in ("/"):  # / not same ?
                assert v[0] == dd[k][0]
                assert v[0] == ddd[k][0]
                compare_stats(v[1], dd[k][1], k)
                compare_stats(v[1], ddd[k][1], k)

        print("compare done")
    finally:
        close_zk_clients(fake_zks)
