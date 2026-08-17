import socket
import struct
import time
import csv
from multiprocessing.dummy import Pool

import pytest
from kazoo.client import KazooClient
from kazoo.retry import KazooRetry

from helpers.cluster_service import RaftKeeperCluster


cluster = RaftKeeperCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper1.xml'],
                             stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_keeper2.xml'],
                             stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_keeper3.xml'],
                             stay_alive=True)

bool_struct = struct.Struct("B")
int_struct = struct.Struct("!i")
int_int_struct = struct.Struct("!ii")
int_int_long_struct = struct.Struct("!iiq")
int_int_int_struct = struct.Struct("!iii")

int_long_int_long_struct = struct.Struct("!iqiq")
long_struct = struct.Struct("!q")
multi_header_struct = struct.Struct("!iBi")
reply_header_struct = struct.Struct("!iqi")
stat_struct = struct.Struct("!qqqqiiiqiiq")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_node(node):
    node.wait_for_join_cluster()


def wait_nodes():
    for n in [node1, node2, node3]:
        wait_node(n)


def get_fake_zk(node_name, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(node_name) + ":8101", timeout=timeout)
    _fake_zk_instance.retry = KazooRetry(ignore_expire=False, max_delay=1.0, max_tries=1)
    _fake_zk_instance.start()
    return _fake_zk_instance


def get_keeper_socket(node_name):
    hosts = cluster.get_instance_ip(node_name)
    client = socket.socket()
    client.settimeout(10)
    client.connect((hosts, 8101))
    return client


def close_keeper_socket(cli):
    if cli is not None:
        cli.close()


def write_buffer(bytes):
    if bytes is None:
        return int_struct.pack(-1)
    else:
        return int_struct.pack(len(bytes)) + bytes


def read_buffer(bytes, offset):
    length = int_struct.unpack_from(bytes, offset)[0]
    offset += int_struct.size
    if length < 0:
        return None, offset
    else:
        index = offset
        offset += length
        return bytes[index: index + length], offset


def handshake(node_name=node1.name, session_timeout=11000, session_id=0):
    client = get_keeper_socket(node_name)
    protocol_version = 0
    last_zxid_seen = 0
    session_passwd = b"\x00" * 16
    read_only = 0

    # Handshake serialize and deserialize code is from 'kazoo.protocol.serialization'.

    # serialize handshake
    req = bytearray()
    req.extend(
        int_long_int_long_struct.pack(
            protocol_version, last_zxid_seen, session_timeout, session_id
        )
    )
    req.extend(write_buffer(session_passwd))
    req.extend([1 if read_only else 0])
    # add header
    req = int_struct.pack(45) + req
    print("handshake request - len:", req.hex(), len(req))

    # send request
    client.send(req)
    data = client.recv(1_000)

    # deserialize response
    print("handshake response - len:", data.hex(), len(data))
    # ignore header
    offset = 4
    proto_version, negotiated_timeout, session_id = int_int_long_struct.unpack_from(
        data, offset
    )
    offset += int_int_long_struct.size
    password, offset = read_buffer(data, offset)
    try:
        read_only = bool_struct.unpack_from(data, offset)[0] == 1
        offset += bool_struct.size
    except struct.error:
        read_only = False

    print("negotiated_timeout - session_id", negotiated_timeout, session_id)
    # assert(negotiated_timeout >= 1000 and negotiated_timeout <= 40000)
    return client


def heartbeat(client):
    length = 8
    xid = 1
    op_num = 11

    # serialize heartbeat
    req = bytearray()
    req.extend(
        int_int_int_struct.pack(
            length, xid, op_num
        )
    )

    print("heartbeat request - len:", req.hex(), len(req))

    # send request
    client.send(req)
    data = client.recv(1_000)
    return data


def test_session_timeout(started_cluster):
    wait_nodes()

    client1 = handshake(node1.name)
    client2 = handshake(node2.name)
    client3 = handshake(node3.name)

    p = Pool(3)
    p.map(heartbeat, [client1, client2, client3])

    time.sleep(9)
    p.map(heartbeat, [client2, client3])

    time.sleep(4)
    assert len(heartbeat(client1)) == 0
    assert len(heartbeat(client2)) > 0
    assert len(heartbeat(client3)) > 0

def test_session_max_min_session_timeout(started_cluster):
    wait_nodes()

    client1 = handshake(node1.name, session_timeout=100)    # under min session timeout, session timeout 1s
    client2 = handshake(node2.name, session_timeout=4000)   # normal, session timeout 4s
    client3 = handshake(node3.name, session_timeout=100000)   # exceeding max session timeout, session timeout 12s

    # all the clients should be alive
    assert len(heartbeat(client1)) > 0
    assert len(heartbeat(client2)) > 0
    assert len(heartbeat(client3)) > 0

    time.sleep(3)
    # 3s after the first heartbeat, client1 session should expire
    assert len(heartbeat(client1)) == 0
    assert len(heartbeat(client2)) > 0
    assert len(heartbeat(client3)) > 0

    time.sleep(6)
    # 6s after the second heartbeat, client2 session should expire
    assert len(heartbeat(client2)) == 0
    assert len(heartbeat(client3)) > 0

    time.sleep(14)
    # 10s after the third heartbeat, client3 session should expire
    assert len(heartbeat(client3)) == 0

def send_4lw_cmd(node_name=node1.name, cmd='ruok'):
    client = None
    try:
        client = get_keeper_socket(node_name)
        client.send(cmd.encode())
        data = client.recv(100_000)
        data = data.decode()
        return data
    finally:
        if client is not None:
            client.close()

def test_invalid_timeout_setting(started_cluster):
    wait_nodes()

    node1.stop_raftkeeper()
    time.sleep(3)
    node1.replace_in_config('/etc/raftkeeper-server/config.d/enable_keeper1.xml', '12000', '200')
    node1.start_raftkeeper(start_wait=True)

    data = send_4lw_cmd(node1.name, cmd='conf')
    reader = csv.reader(data.split('\n'), delimiter='=')
    result = {}

    for row in reader:
        if len(row) != 0:
            print(row)
            result[row[0]] = row[1]

    assert result['operation_timeout_ms'] == '1000'
    assert result['min_session_timeout_ms'] == '1000'
    assert result['max_session_timeout_ms'] == '3600000'

    node1.stop_raftkeeper()
    time.sleep(3)
    node1.replace_in_config('/etc/raftkeeper-server/config.d/enable_keeper1.xml', '200', '12000')
    node1.start_raftkeeper()


def send_raw_request(client, xid, op_num, body=b""):
    """Frame a ZK request: [len][xid][opnum][body] where len covers xid+opnum+body."""
    req = int_struct.pack(8 + len(body)) + int_struct.pack(xid) + int_struct.pack(op_num) + body
    client.send(req)


def recv_reply(client):
    """Read one framed reply: returns (xid, zxid, err, body)."""
    data = b""
    while len(data) < 4:
        chunk = client.recv(100_000)
        if not chunk:
            break
        data += chunk
    assert len(data) >= 4, "Connection closed before reply header"
    length = int_struct.unpack_from(data, 0)[0]
    while len(data) < 4 + length:
        chunk = client.recv(100_000)
        if not chunk:
            break
        data += chunk
    assert len(data) >= 4 + length, "Connection closed mid-reply"
    xid, zxid, err = reply_header_struct.unpack_from(data, 4)
    return xid, zxid, err, data[20 : 4 + length]


def test_unknown_opnum_gets_error_and_connection_survives(started_cluster):
    wait_nodes()

    client = handshake(node1.name)

    # Opnum 16 (Reconfig) is unknown to RaftKeeper: it must answer with a clean
    # error reply instead of dropping the request silently (or the connection).
    send_raw_request(client, xid=100, op_num=16, body=int_struct.pack(0))
    xid, zxid, err, body = recv_reply(client)
    assert xid == 100
    assert err != 0
    assert len(body) == 0

    # The connection must still be usable afterwards.
    send_raw_request(client, xid=101, op_num=11)
    xid, zxid, err, body = recv_reply(client)
    assert xid == 101
    assert err == 0

    close_keeper_socket(client)


def test_ttl_create_rejected_and_stream_stays_aligned(started_cluster):
    wait_nodes()

    client = handshake(node1.name)

    path_buf = write_buffer(b"/ttl_node")
    data_buf = write_buffer(b"ttl_data")
    no_acls = int_struct.pack(0)

    # ZK 3.5+ PERSISTENT_WITH_TTL appends an int64 ttl after the flags. RaftKeeper
    # must consume it, reject the mode, and keep the stream aligned for the next
    # request on the same connection.
    send_raw_request(
        client, xid=200, op_num=1,
        body=path_buf + data_buf + no_acls + int_struct.pack(5) + long_struct.pack(60000),
    )
    xid, zxid, err, body = recv_reply(client)
    assert xid == 200
    assert err == -6  # ZUNIMPLEMENTED

    # Container mode: rejected cleanly (matches ClickHouse Keeper).
    send_raw_request(
        client, xid=201, op_num=1,
        body=path_buf + data_buf + no_acls + int_struct.pack(4),
    )
    xid, zxid, err, body = recv_reply(client)
    assert xid == 201
    assert err == -8  # ZBADARGUMENTS

    # The stream must still be aligned: a normal create succeeds, proving the
    # trailing ttl bytes were consumed rather than parsed as the next request.
    one_world_acl = (
        int_struct.pack(1)
        + int_struct.pack(31)
        + write_buffer(b"world")
        + write_buffer(b"anyone")
    )
    send_raw_request(
        client, xid=202, op_num=1,
        body=write_buffer(b"/plain_node") + write_buffer(b"data") + one_world_acl + int_struct.pack(0),
    )
    xid, zxid, err, body = recv_reply(client)
    assert xid == 202
    assert err == 0

    close_keeper_socket(client)


def test_multi_with_unsupported_subop_returns_error_and_server_survives(started_cluster):
    wait_nodes()

    client = handshake(node1.name)

    # A Multi containing GetACL as a sub-op is unsupported. It must fail with
    # ZBADARGUMENTS and, critically, must NOT abort the server process (which is
    # what used to happen when the validation threw at Raft apply time).
    get_acl_sub = int_struct.pack(6) + bool_struct.pack(0) + int_struct.pack(-1) + write_buffer(b"/some_node")
    multi_terminator = int_struct.pack(-1) + bool_struct.pack(1) + int_struct.pack(-1)
    send_raw_request(client, xid=300, op_num=14, body=get_acl_sub + multi_terminator)
    xid, zxid, err, body = recv_reply(client)
    assert xid == 300
    assert err == -8  # ZBADARGUMENTS

    # The server must still be alive: a write through a full client needs a live
    # leader (the bad multi is applied on the leader before it answers).
    fake_zk = get_fake_zk(node1.name)
    fake_zk.create("/post_multi_survived")
    assert fake_zk.exists("/post_multi_survived") is not None
    fake_zk.stop()

    close_keeper_socket(client)
