from rediswriter.sender import Sender
from rediswriter.config import RedisWriterConfig, TargetRedisConfig, LogLevel
from unittest.mock import patch, MagicMock
from redis.exceptions import ConnectionError
import time

@patch('rediswriter.sender.RedisPipelinePublisher')
def test_simple_publish(redis_publisher_mock):
    publish_mock = MagicMock()
    redis_publisher_mock.return_value.__enter__.return_value = publish_mock
    
    testee = Sender(RedisWriterConfig(
        target_redis=TargetRedisConfig(
            host="localhost", 
            port=6379, 
            buffer_length=10, 
            target_stream_maxlen=1000
        )
    ))

    with testee as publish:
        publish('key', b'msg_bytes')
        publish('key', b'msg_bytes')
        publish('key', b'msg_bytes')
        time.sleep(0.5)
        publish('key', b'msg_bytes')
        time.sleep(0.5)

    # Verify that two batches were send of length 3 and 1
    assert publish_mock.call_count == 2
    assert len(publish_mock.call_args_list[0].args[0]) == 3
    assert len(publish_mock.call_args_list[1].args[0]) == 1    

@patch('rediswriter.sender.RedisPipelinePublisher')
def test_error_publish(redis_publisher_mock):
    publish_mock = MagicMock()
    redis_publisher_mock.return_value.__enter__.return_value = publish_mock

    publish_mock.side_effect = [
        None,
        ConnectionError(),
        ConnectionError(),
        ConnectionError(),
        ConnectionError(),
        ConnectionError(),
        ConnectionError(),
        None,
    ]
    
    testee = Sender(RedisWriterConfig(
        target_redis=TargetRedisConfig(
            host="localhost", 
            port=6379, 
            buffer_length=10, 
            target_stream_maxlen=1000
        )
    ))

    with testee as publish:
        publish('key1', b'')
        time.sleep(0.5)
        publish('key2', b'')
        time.sleep(0.5)

    # Verify that all messages were sent (and key2 was retried)
    assert publish_mock.call_count == 5
    assert publish_mock.call_args_list[0].args[0][0].stream_key == 'key1'
    assert publish_mock.call_args_list[1].args[0][0].stream_key == 'key2'
    assert publish_mock.call_args_list[2].args[0][0].stream_key == 'key2'

