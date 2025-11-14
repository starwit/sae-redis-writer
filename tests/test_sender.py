import time
from collections import Counter
from unittest.mock import MagicMock, patch

import pytest
from redis.exceptions import ConnectionError

from rediswriter.config import (MappingConfig, RedisWriterConfig,
                                TargetRedisConfig)
from rediswriter.sender import Sender


@pytest.fixture
def config():
    return RedisWriterConfig(
        target_redis=TargetRedisConfig(
            host="localhost", 
            port=6379, 
            buffer_length=10, 
            target_stream_maxlen=1000
        ),
        mapping_config=[MappingConfig()]
    )

@patch('rediswriter.sender.RedisPipelinePublisher')
def test_simple_publish(redis_publisher_mock, config):
    publish_mock = MagicMock()
    redis_publisher_mock.return_value.__enter__.return_value = publish_mock
    
    testee = Sender(config)

    with testee as publish:
        publish('key', b'msg_bytes')
        publish('key', b'msg_bytes')
        publish('key', b'msg_bytes')
        time.sleep(0.1)
        publish('key', b'msg_bytes')
        time.sleep(0.1)

    # Verify that two batches were send of length 3 and 1
    assert publish_mock.call_count == 2
    assert len(publish_mock.call_args_list[0].args[0]) == 3
    assert len(publish_mock.call_args_list[1].args[0]) == 1    

@patch('rediswriter.sender.RedisPipelinePublisher')
def test_error_publish(redis_publisher_mock, config):
    publish_mock = MagicMock()
    redis_publisher_mock.return_value.__enter__.return_value = publish_mock

    publish_mock.side_effect = [
        None,
        ConnectionError(),
        None,
    ]
    
    testee = Sender(config)

    with testee as publish:
        publish('key1', b'')
        time.sleep(0.1)
        publish('key2', b'')
        time.sleep(0.1)

    # Verify that all messages were sent (and key2 was retried)
    assert publish_mock.call_count == 3
    assert publish_mock.call_args_list[0].args[0][0].stream_key == 'key1'
    assert publish_mock.call_args_list[1].args[0][0].stream_key == 'key2'
    assert publish_mock.call_args_list[2].args[0][0].stream_key == 'key2'

@patch('rediswriter.sender.RedisPipelinePublisher')
def test_error_backoff_reset(redis_publisher_mock, config):
    publish_mock = MagicMock()
    redis_publisher_mock.return_value.__enter__.return_value = publish_mock

    publish_mock.side_effect = [
        None,
        ConnectionError(),
        ConnectionError(),
        ConnectionError(),
        ConnectionError(),
        None,
        ConnectionError(),
        None,
    ]
    
    testee = Sender(config)

    with testee as publish:
        publish('key1', b'')
        time.sleep(0.1)
        publish('key2', b'')
        time.sleep(0.8)
        publish('key3', b'')
        time.sleep(0.1)


    # Verify that all messages were sent. If backoff was not reset, the sleep time would not be enough to make all calls.
    assert publish_mock.call_count == 8

    send_counts = Counter(map(lambda c: c.args[0][0].stream_key, publish_mock.call_args_list))
    assert send_counts['key1'] == 1
    assert send_counts['key2'] == 5
    assert send_counts['key3'] == 2


@patch('rediswriter.sender.RedisPipelinePublisher')
def test_bug_all_dropped_1904(redis_publisher_mock, config):
    publish_mock = MagicMock()
    redis_publisher_mock.return_value.__enter__.return_value = publish_mock

    # We can't know because we don't have any logs, but the theory is that an unexpected exception crashed the sender thread
    publish_mock.side_effect = [
        Exception(),
        None,
    ]
    
    testee = Sender(config)

    with testee as publish:
        time.sleep(0.2)
        publish('key1', b'')
        time.sleep(0.2)

    # Verify that 'key1' message has been sent after the unexpected exception
    assert len(publish_mock.mock_calls) == 2
    assert publish_mock.call_args_list[1].args[0][0].stream_key == 'key1'
