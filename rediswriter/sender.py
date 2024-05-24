import logging
import time
from collections import deque
from threading import Event, Thread
from typing import Deque, List, NamedTuple, Tuple

import backoff
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

def _on_backoff(_):
    logger.debug(f'Failed to send message. Backing off...')
    BACKOFF_COUNTER.inc()


class BufferEntry(NamedTuple):
    stream_key: str
    msg_bytes: bytes


class Sender:
    def __init__(self, config: RedisWriterConfig) -> None:
        self._config = config.target_redis
        self._stream_ids = config.stream_ids
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

        with publisher as publish:
            while not self._stop_event.is_set():
                batch = self._get_next_batch()
                    
                if len(batch) == 0:
                    time.sleep(0.05)
                    continue

                try:
                    self._publish_with_backoff(publish, batch)
                except (ConnectionError, TimeoutError) as e:
                    logger.error(f'Gave up sending message', exc_info=True)
                    GIVEUP_COUNTER.inc()

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

    @backoff.on_exception(
            backoff.expo, 
            exception=(ConnectionError, TimeoutError), 
            max_tries=7,
            on_backoff=_on_backoff,
            logger=None,
            factor=0.1, 
            max_value=5,
    )
    def _publish_with_backoff(self, publish: callable, stream_entries: List[Tuple[str, bytes]]):
        with REDIS_PUBLISH_DURATION.time():
            publish(stream_entries)

    def __exit__(self, _, __, ___):
        self._stop_event.set()
        self._sender_thread.join(timeout=10)
        return False