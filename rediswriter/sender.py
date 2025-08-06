import logging
import time
from collections import deque
from threading import Event, Thread
from typing import Deque, List, NamedTuple, Tuple

from prometheus_client import Counter, Histogram, Summary
from redis.exceptions import ConnectionError, TimeoutError
from visionlib.pipeline.publisher import RedisPipelinePublisher

from .config import RedisWriterConfig

logging.basicConfig(format='%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s')
logger = logging.getLogger(__name__)

BACKOFF_COUNTER = Counter('redis_writer_backoff_counter', 'How often publishing to Redis has to be backed off (i.e. retried)')
GIVEUP_COUNTER = Counter('redis_writer_giveup_counter', 'How many messages were discarded due to exhausted retries')
DISCARD_BUFFER_COUNTER = Counter('redis_writer_discard_buffer_counter', 'How many input messages have to be discarded because sender cannot keep up')
REDIS_PUBLISH_DURATION = Histogram('redis_writer_target_redis_publish_duration', 'The time it takes to push a message onto the Redis stream',
                                   buckets=(0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25))
REDIS_PUBLISH_BYTES_SENT = Summary('redis_writer_target_redis_published_bytes_estimate', 'How many bytes were sent to the Redis stream (this is estimated!)')
REDIS_PUBLISH_MESSAGE_COUNT = Counter('redis_writer_target_redis_message_counter', 'How many messages were sent to the Redis stream')

def backoff_gen(max_wait=10):
    wait_time = 0.05
    while True:
        yield wait_time
        wait_time = min(wait_time*2, max_wait)

class BufferEntry(NamedTuple):
    stream_key: str
    msg_bytes: bytes


class Sender:
    def __init__(self, config: RedisWriterConfig) -> None:
        self._config = config.target_redis
        logger.setLevel(config.log_level.value)

        self._buffer: Deque[BufferEntry] = deque(maxlen=self._config.buffer_length)

        self._stop_event = Event()
        self._sender_thread = Thread(target=self._run)

        self._redis_args = {
            'ssl': True,
            'ssl_certfile': 'certs/client.crt',
            'ssl_keyfile': 'certs/client.key',
            'ssl_cert_reqs': 'required',
            'ssl_ca_certs': 'certs/ca.crt',
        } if self._config.tls else {}
        
    def _publish(self, stream_key, msg_bytes):
        if len(self._buffer) == self._buffer.maxlen:
            DISCARD_BUFFER_COUNTER.inc()

        self._buffer.append(BufferEntry(stream_key, msg_bytes))

    def __enter__(self):
        self._sender_thread.start()
        return self._publish
    
    def _run(self):
        publisher = RedisPipelinePublisher(
            host=self._config.host,
            port=self._config.port,
            stream_maxlen=self._config.target_stream_maxlen,
            **self._redis_args
        )

        backoff_time = backoff_gen()
        connection_healthy = True
        batch = []

        with publisher as publish:
            while not self._stop_event.is_set():
                if connection_healthy:
                    batch = self._get_next_batch()
                    
                if len(batch) == 0:
                    time.sleep(0.05)
                    continue

                try:
                    with REDIS_PUBLISH_DURATION.time():
                        publish(batch)
                    if not connection_healthy:
                        connection_healthy = True
                        backoff_time = backoff_gen()
                        logger.info(f'Connection to {self._config.host}:{self._config.port} healthy. Resuming.')
                        
                except (ConnectionError, TimeoutError) as e:
                    connection_healthy = False

                if not connection_healthy:
                    sleep_time = next(backoff_time)
                    logger.warning(f'Connection unhealthy, retrying in {sleep_time}s...')
                    BACKOFF_COUNTER.inc()
                    time.sleep(sleep_time)

    def _get_next_batch(self):
        batch = []
        batch_bytes = 0
        try:
            while len(batch) < self._config.buffer_length: 
                entry = self._buffer.popleft()
                batch.append(entry)
                # Assume 33% overhead for b64 encoding
                batch_bytes += round(len(entry.msg_bytes) * 1.33) + len(entry.stream_key)
        except IndexError:
            pass

        if len(batch) > 0:
            REDIS_PUBLISH_BYTES_SENT.observe(batch_bytes)
            REDIS_PUBLISH_MESSAGE_COUNT.inc(len(batch))

        return batch

    def __exit__(self, _, __, ___):
        self._stop_event.set()
        self._sender_thread.join(timeout=10)
        return False