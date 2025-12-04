import logging
import signal
import threading
from enum import Enum
from typing import Dict

from prometheus_client import Counter, start_http_server
from visionapi.common_pb2 import TypeMessage, MessageType
from visionlib.pipeline import ValkeyConsumer

from .config import RedisWriterConfig
from .rediswriter import RedisWriter
from .sender import Sender

logger = logging.getLogger(__name__)

FRAME_COUNTER = Counter('redis_writer_frame_counter', 'How many frames have been consumed from the Redis input stream')

def run_stage():

    stop_event = threading.Event()

    # Register signal handlers
    def sig_handler(signum, _):
        signame = signal.Signals(signum).name
        print(f'Caught signal {signame} ({signum}). Exiting...')
        stop_event.set()

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    # Load config from settings.yaml / env vars
    CONFIG = RedisWriterConfig()

    logger.setLevel(CONFIG.log_level.value)

    logger.info(f'Starting prometheus metrics endpoint on port {CONFIG.prometheus_port}')

    start_http_server(CONFIG.prometheus_port)

    logger.info(f'Starting redis writer stage. Config: {CONFIG.model_dump_json(indent=2)}')

    redis_writer = RedisWriter(CONFIG)
    
    stream_mapping = map_config(CONFIG.mapping_config)
    
    consumer_ctx = ValkeyConsumer(CONFIG.redis.host, CONFIG.redis.port, stream_mapping.keys())
    logger.debug(f"Listening to stream keys {stream_mapping.keys()}")
    
    sender = Sender(CONFIG)

    message_type_by_stream: Dict[str, MessageType] = {}
    
    with consumer_ctx as iter_messages, sender as send:
        for stream_key, proto_data in iter_messages():
            if stop_event.is_set():
                break

            if stream_key is None:
                continue

            stream_id = stream_key.split(':')[1]            

            FRAME_COUNTER.inc()
            logger.debug(f'Received message on stream {stream_id}')

            # Detect stream type by analyzing first message
            if not stream_id in message_type_by_stream:                
                msg = TypeMessage()
                msg.ParseFromString(proto_data)
                if msg.type != MessageType.UNSPECIFIED:
                    message_type_by_stream[stream_id] = msg.type
                else:
                    # if message type can't be determined, messages are NOT forwarded
                    continue
                
                type = MessageType.Name(msg.type)
                logger.info(f'Detected message type {type} on stream {stream_id}')

            # Only process SaeMessage messages, otherwise pass verbatim
            if message_type_by_stream[stream_id] == MessageType.SAE:
                output_proto_data = redis_writer.get(proto_data)
            else:
                output_proto_data = proto_data

            if output_proto_data is None:
                continue

            target_stream = stream_mapping.get(stream_key)
            send(target_stream, output_proto_data)

            
def map_config(mapping_config):
    stream_mapping = {}
    for mapping in mapping_config:
        if mapping.source == None:
            continue
        if mapping.target == None:
            stream_mapping[mapping.source] = mapping.source
        else:            
            stream_mapping[mapping.source] = mapping.target
    return stream_mapping