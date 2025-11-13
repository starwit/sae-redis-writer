import base64
import os
import random
import subprocess
import sys
import time
import uuid

import pytest
import redis
from testcontainers.core.container import DockerContainer
from visionapi.common_pb2 import MessageType
from visionapi.sae_pb2 import Detection, SaeMessage


@pytest.fixture(scope="module")
def redis_container():
    with DockerContainer("redis:7.2-alpine").with_exposed_ports(6379) as redis:
        yield redis

@pytest.fixture(scope="module")
def target_redis_container():
    with DockerContainer("redis:7.2-alpine").with_exposed_ports(6379).with_kwargs(cap_add=["NET_ADMIN"]) as redis:
        redis.exec("apk add iproute2")
        redis.exec("tc qdisc add dev eth0 root netem delay 30ms")
        yield redis

@pytest.fixture(scope="module")
def target_redis_client(target_redis_container):
    return redis.Redis(host=target_redis_container.get_container_host_ip(), port=target_redis_container.get_exposed_port(6379))

@pytest.fixture(scope="module")
def redis_client(redis_container):
    return redis.Redis(host=redis_container.get_container_host_ip(), port=redis_container.get_exposed_port(6379))

@pytest.fixture(autouse=True)
def cleanup_redis(redis_client, target_redis_client):
    yield
    redis_client.flushall()
    target_redis_client.flushall()

@pytest.fixture(autouse=True)
def wait_for_writer_connection(redis_client):
    timeout_time = time.time() + 10
    while time.time() < timeout_time:
        if len(redis_client.client_list()) >= 2:
            break
    yield

@pytest.fixture(scope="module")
def writer_stage(redis_container, target_redis_container):
    CONFIG_ENV = {
        "REDIS__HOST": str(redis_container.get_container_host_ip()),
        "REDIS__PORT": str(redis_container.get_exposed_port(6379)),
        "TARGET_REDIS__HOST": str(target_redis_container.get_container_host_ip()),
        "TARGET_REDIS__PORT": str(target_redis_container.get_exposed_port(6379)),
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
def test_single_message_send(writer_stage, redis_client, target_redis_client):
    '''Create a SaeMessage and feed it into the redis writer'''
    sae_msg_bytes = create_sae_msg(1)

    redis_client.xadd("input:stream", {"proto_data_b64": sae_msg_bytes})

    time.sleep(1)

    # Check that the message was written to the target redis
    messages = target_redis_client.xrange("output:stream")
    assert len(messages) == 1
    assert_redis_msg(1, messages[0])  

@pytest.mark.integration
def test_many_messages_send(writer_stage, redis_client, target_redis_client):
    '''Create many SaeMessages and feed them into the redis writer'''
    MSG_COUNT = 1000
    for i in range(MSG_COUNT):
        sae_msg_bytes = create_sae_msg(i)
        redis_client.xadd("input:stream", {"proto_data_b64": sae_msg_bytes})

    time.sleep(2)

    # Check that the messages were written to the target redis
    messages = target_redis_client.xrange("output:stream")
    assert len(messages) == MSG_COUNT
    for i, message in enumerate(messages):
        assert_redis_msg(i, message)
