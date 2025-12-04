import base64
import random
import subprocess
import sys
import time
import uuid

import pytest
import valkey
from testcontainers.core.container import DockerContainer
from visionapi.common_pb2 import MessageType
from visionapi.sae_pb2 import Detection, SaeMessage


@pytest.fixture(scope='module')
def valkey_container():
    with DockerContainer('valkey/valkey:9-alpine').with_exposed_ports(6379) as valkey:
        yield valkey

@pytest.fixture(scope='module')
def target_valkey_container():
    with DockerContainer('valkey/valkey:9-alpine').with_exposed_ports(6379).with_kwargs(cap_add=['NET_ADMIN']) as valkey:
        valkey.exec('apk add iproute2')
        yield valkey

@pytest.fixture(scope='module')
def fail_network(target_valkey_container):
    def _fail_network():
        target_valkey_container.exec('tc qdisc add dev eth0 root netem loss 100%')
    return _fail_network

@pytest.fixture(scope='module')
def restore_network(target_valkey_container):
    def _restore_network():
        target_valkey_container.exec('tc qdisc del dev eth0 root')
    return _restore_network

@pytest.fixture(scope='module')
def target_valkey_client(target_valkey_container):
    return valkey.Valkey(host=target_valkey_container.get_container_host_ip(), port=target_valkey_container.get_exposed_port(6379))

@pytest.fixture(scope='module')
def valkey_client(valkey_container):
    return valkey.Valkey(host=valkey_container.get_container_host_ip(), port=valkey_container.get_exposed_port(6379))
    
@pytest.fixture()
def set_network_delay(target_valkey_container):
    def _set_network_delay(delay_ms: int, jitter_ms: int = 0, correlation_pct: int = 0):
        target_valkey_container.exec(f'tc qdisc add dev eth0 root netem delay {delay_ms}ms {jitter_ms}ms {correlation_pct}%')
    return _set_network_delay

@pytest.fixture(autouse=True)
def cleanup_valkey(valkey_client, target_valkey_client):
    yield
    valkey_client.flushall()
    target_valkey_client.flushall()

@pytest.fixture(autouse=True)
def cleanup_netem(target_valkey_container):
    yield
    target_valkey_container.exec('tc qdisc del dev eth0 root')

@pytest.fixture(autouse=True)
def wait_for_writer_connection(valkey_client):
    timeout_time = time.time() + 10
    while time.time() < timeout_time:
        if len(valkey_client.client_list()) >= 2:
            break
    yield

@pytest.fixture(scope='module', autouse=True)
def writer_stage(valkey_container, target_valkey_container):
    CONFIG_ENV = {
        'LOG_LEVEL': 'INFO',
        'REDIS__HOST': str(valkey_container.get_container_host_ip()),
        'REDIS__PORT': str(valkey_container.get_exposed_port(6379)),
        'TARGET_REDIS__HOST': str(target_valkey_container.get_container_host_ip()),
        'TARGET_REDIS__PORT': str(target_valkey_container.get_exposed_port(6379)),
        'TARGET_REDIS__BUFFER_LENGTH': '1000',
        'TARGET_REDIS__TARGET_STREAM_MAXLEN': '10000',
        'TARGET_REDIS__SOCKET_TIMEOUT_S': '1',
        'MAPPING_CONFIG': '[{\"source\": \"input:stream\", \"target\": \"output:stream\"}]',
    }
    proc = subprocess.Popen([sys.executable, 'main.py'], stdout=sys.stdout, stderr=sys.stderr, env=CONFIG_ENV)
    yield
    proc.terminate()
    proc.wait()

def create_sae_det():
    sae_det = Detection()
    sae_det.bounding_box.min_x = random.randint(0, 1000)
    sae_det.bounding_box.min_y = random.randint(0, 1000)
    sae_det.bounding_box.max_x = random.randint(0, 1000)
    sae_det.bounding_box.max_y = random.randint(0, 1000)
    sae_det.confidence = random.randint(0, 100) / 100
    sae_det.class_id = 1
    sae_det.object_id = uuid.uuid4().bytes
    return sae_det

def create_sae_msg(ts: int):
    sae_msg = SaeMessage()
    sae_msg.frame.source_id = 'source_id'
    sae_msg.frame.timestamp_utc_ms = ts
    sae_msg.frame.shape.width = 1920
    sae_msg.frame.shape.height = 1080
    sae_msg.frame.shape.channels = 3
    sae_msg.frame.frame_data = b'1234567890'
    sae_msg.type = MessageType.SAE

    for _ in range(random.randint(2, 20)):
        sae_msg.detections.append(create_sae_det())

    return base64.b64encode(sae_msg.SerializeToString())

def assert_redis_msg(ts: int, redis_msg):
    output_msg = SaeMessage()
    output_msg.ParseFromString(base64.b64decode(redis_msg[1][b'proto_data_b64']))
    assert output_msg.frame.timestamp_utc_ms == ts

@pytest.mark.integration
def test_single_message_send(valkey_client, target_valkey_client):
    '''Create a SaeMessage and feed it into the redis writer'''
    sae_msg_bytes = create_sae_msg(1)

    valkey_client.xadd('input:stream', {'proto_data_b64': sae_msg_bytes})

    time.sleep(1)

    # Check that the message was written to the target redis
    messages = target_valkey_client.xrange('output:stream')
    assert len(messages) == 1
    assert_redis_msg(1, messages[0])  

@pytest.mark.integration
def test_many_messages_send(valkey_client, target_valkey_client, set_network_delay):
    '''Create many SaeMessages and feed them into the redis writer with some realistic network delay'''
    set_network_delay(50, 20)

    MSG_COUNT = 1000
    for i in range(MSG_COUNT):
        sae_msg_bytes = create_sae_msg(i)
        valkey_client.xadd('input:stream', {'proto_data_b64': sae_msg_bytes})

    time.sleep(2)

    # Check that the messages were written to the target redis
    messages = target_valkey_client.xrange('output:stream')
    assert len(messages) == MSG_COUNT
    for i, message in enumerate(messages):
        assert_redis_msg(i, message)

@pytest.mark.integration
def test_network_outage(valkey_client, target_valkey_client, fail_network, restore_network):
    '''Create many SaeMessages and feed them into the redis writer, simulating a network outage'''

    message_id = 0

    def send_messages(count: int):
        nonlocal message_id
        for _ in range(count):
            sae_msg_bytes = create_sae_msg(message_id)
            valkey_client.xadd('input:stream', {'proto_data_b64': sae_msg_bytes})
            message_id += 1

    # Warmup
    send_messages(100)

    # Send messages under failed network conditions
    fail_network()
    send_messages(100)
    
    time.sleep(5)

    # Restore network, send some more messages and allow some time for delivery
    restore_network()
    send_messages(100)

    time.sleep(2)

    # Check that the messages were written to the target redis
    messages = target_valkey_client.xrange('output:stream')
    assert len(messages) == message_id
    for i, message in enumerate(messages):
        assert_redis_msg(i, message)