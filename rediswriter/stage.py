import logging
import signal
import threading
from enum import Enum
from typing import Dict

from prometheus_client import Counter, start_http_server
from visionapi.sae_pb2 import SaeMessage
from visionlib.pipeline.consumer import RedisConsumer
from visionlib.pipeline.formats import is_sae_message

from .config import RedisWriterConfig
from .rediswriter import RedisWriter
from .sender import Sender

logger = logging.getLogger(__name__)

FRAME_COUNTER = Counter('redis_writer_frame_counter', 'How many frames have been consumed from the Redis input stream')

class MessageType(Enum):
    SAE = 1
    OTHER = 2

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

    consumer = RedisConsumer(CONFIG.redis.host, CONFIG.redis.port, 
                            stream_keys=[f'{CONFIG.redis.input_stream_prefix}:{id}' for id in CONFIG.stream_ids])
    
    sender = Sender(CONFIG)

    message_type_by_stream: Dict[str, MessageType] = {}
    
    with consumer as consume, sender as send:
        for stream_key, proto_data in consume():
            if stop_event.is_set():
                break

            if stream_key is None:
                continue

            stream_id = stream_key.split(':')[1]

            FRAME_COUNTER.inc()

            # Detect stream type by analyzing first message
            if not stream_id in message_type_by_stream:
                msg = SaeMessage()
                msg.ParseFromString(proto_data)
                if is_sae_message(msg):
                    msg_type = MessageType.SAE
                else:
                    msg_type = MessageType.OTHER
                message_type_by_stream[stream_id] = msg_type
                logger.info(f'Detected message type {msg_type.name} on stream {stream_id}')

            # Only process SaeMessage messages, otherwise pass verbatim
            if message_type_by_stream[stream_id] == MessageType.SAE:
                output_proto_data = redis_writer.get(proto_data)
            else:
                output_proto_data = proto_data

            if output_proto_data is None:
                continue

            send(f'{CONFIG.target_redis.output_stream_prefix}:{stream_id}', output_proto_data)