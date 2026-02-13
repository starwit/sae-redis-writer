from typing import List
from unittest.mock import patch

import pytest
from visionapi.common_pb2 import MessageType
from visionapi.sae_pb2 import SaeMessage, PositionMessage

from rediswriter.config import RedisWriterConfig, MappingConfig, TargetRedisConfig
from rediswriter.stage import run_stage


@pytest.fixture(autouse=True)
def disable_prometheus():
    # We don't want to start the Prometheus server during tests
    with patch('rediswriter.stage.start_http_server'):
        yield

@pytest.fixture
def set_config():
    with patch('rediswriter.stage.RedisWriterConfig') as mock_config:
        def _make_mock_config(mappings: List[MappingConfig]):
            mock_config.return_value = RedisWriterConfig(
                log_level='WARNING',
                mapping_config=mappings,
                target_redis=TargetRedisConfig(host='dummy', port=1234)
            )
        yield _make_mock_config

@pytest.fixture
def sender_mock():
    with patch('rediswriter.stage.Sender') as mock_sender:
        yield mock_sender.return_value.__enter__.return_value

@pytest.fixture
def inject_consumer_messages():
    with patch('rediswriter.stage.ValkeyConsumer') as mock_consumer:
        def _inject_messages(messages):
            mock_consumer.return_value.__enter__.return_value.return_value.__iter__.return_value = iter(messages)
        yield _inject_messages

def test_frame_data_removal(set_config, sender_mock, inject_consumer_messages):
    set_config(mappings=[
        MappingConfig(source='stage:test_stream', target='stage:test_stream_copy'),
    ])
    
    inject_consumer_messages([
        ('stage:test_stream', _make_sae_msg_bytes(1)),
        ('stage:test_stream', _make_sae_msg_bytes(2)),
    ])
    
    # Run the stage (this will process the injected messages)
    run_stage()
    
    # Verify that messages were published
    assert sender_mock.call_count == 2
    assert sender_mock.call_args_list[0].args[0] == 'stage:test_stream_copy'
    assert sender_mock.call_args_list[1].args[0] == 'stage:test_stream_copy'
    _assert_sae_message(sender_mock.call_args_list[0].args[1], timestamp=1, no_frame_data=True)
    _assert_sae_message(sender_mock.call_args_list[1].args[1], timestamp=2, no_frame_data=True)

def test_forward(set_config, sender_mock, inject_consumer_messages):
    set_config(mappings=[
        MappingConfig(source='stage:test_stream', target='stage:test_stream_copy'),
        MappingConfig(source='stage2:test_stream', target='stage2:test_stream_copy'),
    ])
    
    inject_consumer_messages([
        ('stage:test_stream', _make_position_msg_bytes(1)),
        ('stage2:test_stream', _make_position_msg_bytes(2)),
    ])
    
    # Run the stage (this will process the injected messages)
    run_stage()
    
    # Verify that messages were published
    assert sender_mock.call_count == 2
    assert sender_mock.call_args_list[0].args[0] == 'stage:test_stream_copy'
    assert sender_mock.call_args_list[1].args[0] == 'stage2:test_stream_copy'
    _assert_position_message(sender_mock.call_args_list[0].args[1], timestamp=1)
    _assert_position_message(sender_mock.call_args_list[1].args[1], timestamp=2)
    
def test_bug_frame_data_removal_fails_with_multiple_streams_2115(set_config, sender_mock, inject_consumer_messages):
    set_config(mappings=[
        MappingConfig(source='stage:test_stream', target='stage:test_stream_copy'),
        MappingConfig(source='stage2:test_stream', target='stage2:test_stream_copy'),
    ])
    
    inject_consumer_messages([
        ('stage:test_stream', _make_position_msg_bytes(1)),
        ('stage2:test_stream', _make_sae_msg_bytes(2)),
    ])
    
    # Run the stage (this will process the injected messages)
    run_stage()
    
    # Verify that messages were published
    assert sender_mock.call_count == 2
    assert sender_mock.call_args_list[0].args[0] == 'stage:test_stream_copy'
    assert sender_mock.call_args_list[1].args[0] == 'stage2:test_stream_copy'
    _assert_position_message(sender_mock.call_args_list[0].args[1], timestamp=1)
    _assert_sae_message(sender_mock.call_args_list[1].args[1], timestamp=2, no_frame_data=True)

def _assert_sae_message(sae_msg_bytes: bytes, timestamp: int, no_frame_data: bool):
    sae_msg = SaeMessage()
    sae_msg.ParseFromString(sae_msg_bytes)
    assert sae_msg.frame.timestamp_utc_ms == timestamp

    if no_frame_data:
        assert all((
            len(sae_msg.frame.frame_data) == 0,
            len(sae_msg.frame.frame_data_jpeg) == 0
        ))

def _assert_position_message(pos_msg_bytes: bytes, timestamp: int):
    pos_msg = PositionMessage()
    pos_msg.ParseFromString(pos_msg_bytes)
    assert pos_msg.timestamp_utc_ms == timestamp

def _make_sae_msg_bytes(timestamp: int) -> bytes:
    sae_msg = SaeMessage()
    sae_msg.frame.timestamp_utc_ms = timestamp
    sae_msg.frame.frame_data = b'dummy_frame_data'
    sae_msg.frame.frame_data_jpeg = b'dummy_frame_data_jpeg'
    sae_msg.type = MessageType.SAE
    return sae_msg.SerializeToString()

def _make_position_msg_bytes(timestamp: int) -> bytes:
    pos_msg = PositionMessage()
    pos_msg.timestamp_utc_ms = timestamp
    pos_msg.type = MessageType.POSITION
    return pos_msg.SerializeToString()