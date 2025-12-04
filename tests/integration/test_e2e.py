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


@pytest.fixture(scope="module")
def valkey_container():
    with DockerContainer("valkey/valkey:9-alpine").with_exposed_ports(6379) as valkey:
        yield valkey

@pytest.fixture(scope="module")
def target_valkey_container():
    with DockerContainer("valkey/valkey:9-alpine").with_exposed_ports(6379).with_kwargs(cap_add=["NET_ADMIN"]) as valkey:
        valkey.exec("apk add iproute2")
        valkey.exec("tc qdisc add dev eth0 root netem delay 30ms")
        yield valkey

@pytest.fixture(scope="module")
def target_valkey_client(target_valkey_container):
    return valkey.Valkey(host=target_valkey_container.get_container_host_ip(), port=target_valkey_container.get_exposed_port(6379))

@pytest.fixture(scope="module")
def valkey_client(valkey_container):
    return valkey.Valkey(host=valkey_container.get_container_host_ip(), port=valkey_container.get_exposed_port(6379))
    
@pytest.fixture(autouse=True)
def cleanup_valkey(valkey_client, target_valkey_client):
    yield
    valkey_client.flushall()
    target_valkey_client.flushall()

@pytest.fixture(autouse=True)
def wait_for_writer_connection(valkey_client):
    timeout_time = time.time() + 10
    while time.time() < timeout_time:
        if len(valkey_client.client_list()) >= 2:
            break
    yield

@pytest.fixture(scope="module")
def writer_stage(valkey_container, target_valkey_container):
    CONFIG_ENV = {
        "REDIS__HOST": str(valkey_container.get_container_host_ip()),
        "REDIS__PORT": str(valkey_container.get_exposed_port(6379)),
        "TARGET_REDIS__HOST": str(target_valkey_container.get_container_host_ip()),
        "TARGET_REDIS__PORT": str(target_valkey_container.get_exposed_port(6379)),
        "TARGET_REDIS__BUFFER_LENGTH": "1000",
        "TARGET_REDIS__TARGET_STREAM_MAXLEN": "10000",
        "MAPPING_CONFIG": "[{\"source\": \"input:stream\", \"target\": \"output:stream\"}]",
    }
    proc = subprocess.Popen([sys.executable, "main.py"], stdout=sys.stdout, stderr=sys.stderr, env=CONFIG_ENV)
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
    sae_msg.frame.source_id = "source_id"
    sae_msg.frame.timestamp_utc_ms = ts
    sae_msg.frame.shape.width = 1920
    sae_msg.frame.shape.height = 1080
    sae_msg.frame.shape.channels = 3
    sae_msg.frame.frame_data = b"1234567890"
    sae_msg.type = MessageType.SAE

    for _ in range(random.randint(2, 20)):
        sae_msg.detections.append(create_sae_det())

    return base64.b64encode(sae_msg.SerializeToString())

def assert_redis_msg(ts: int, redis_msg):
    output_msg = SaeMessage()
    output_msg.ParseFromString(base64.b64decode(redis_msg[1][b"proto_data_b64"]))
    assert output_msg.frame.timestamp_utc_ms == ts

@pytest.mark.integration
def test_single_message_send(writer_stage, valkey_client, target_valkey_client):
    '''Create a SaeMessage and feed it into the redis writer'''
    sae_msg_bytes = create_sae_msg(1)

    valkey_client.xadd("input:stream", {"proto_data_b64": sae_msg_bytes})

    time.sleep(1)

    # Check that the message was written to the target redis
    messages = target_valkey_client.xrange("output:stream")
    assert len(messages) == 1
    assert_redis_msg(1, messages[0])  

@pytest.mark.integration
def test_many_messages_send(writer_stage, valkey_client, target_valkey_client):
    '''Create many SaeMessages and feed them into the redis writer'''
    MSG_COUNT = 1000
    for i in range(MSG_COUNT):
        sae_msg_bytes = create_sae_msg(i)
        valkey_client.xadd("input:stream", {"proto_data_b64": sae_msg_bytes})

    time.sleep(2)

    # Check that the messages were written to the target redis
    messages = target_valkey_client.xrange("output:stream")
    assert len(messages) == MSG_COUNT
    for i, message in enumerate(messages):
        assert_redis_msg(i, message)
