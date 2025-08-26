import logging
from typing import Any

from prometheus_client import Histogram, Summary
from visionapi.sae_pb2 import SaeMessage

from .config import RedisWriterConfig

logging.basicConfig(format='%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s')
logger = logging.getLogger(__name__)

GET_DURATION = Histogram('redis_writer_get_duration', 'The time it takes to deserialize the proto until returning the tranformed result as a serialized proto',
                         buckets=(0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25))
PROTO_SERIALIZATION_DURATION = Summary('redis_writer_proto_serialization_duration', 'The time it takes to create a serialized output proto')
PROTO_DESERIALIZATION_DURATION = Summary('redis_writer_proto_deserialization_duration', 'The time it takes to deserialize an input proto')


class RedisWriter:
    def __init__(self, config: RedisWriterConfig) -> None:
        self.config = config
        logger.setLevel(self.config.log_level.value)

    def __call__(self, input_proto) -> Any:
        return self.get(input_proto)
    
    @GET_DURATION.time()
    def get(self, input_proto):
        sae_msg = self._unpack_proto(input_proto)

        if self.config.remove_frame_data == True:
            sae_msg = self._remove_frame_data(sae_msg)

        return self._pack_proto(sae_msg)
    
    def _remove_frame_data(self, sae_msg: SaeMessage) -> SaeMessage:
        # Use a whitelist approach, to make 100% sure that no frame_data is leaked
        source_id = sae_msg.frame.source_id
        timestamp_utc_ms = sae_msg.frame.timestamp_utc_ms
        shape = sae_msg.frame.shape
        camera_location = sae_msg.frame.camera_location

        sae_msg.ClearField('frame')

        # Add back some metadata we need downstream
        sae_msg.frame.source_id = source_id
        sae_msg.frame.timestamp_utc_ms = timestamp_utc_ms
        sae_msg.frame.shape.CopyFrom(shape)
        sae_msg.frame.camera_location.CopyFrom(camera_location)

        return sae_msg
        
    @PROTO_DESERIALIZATION_DURATION.time()
    def _unpack_proto(self, sae_message_bytes):
        sae_msg = SaeMessage()
        sae_msg.ParseFromString(sae_message_bytes)

        return sae_msg
    
    @PROTO_SERIALIZATION_DURATION.time()
    def _pack_proto(self, sae_msg: SaeMessage):
        return sae_msg.SerializeToString()