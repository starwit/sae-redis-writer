from typing import List

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Annotated
from visionlib.pipeline.settings import LogLevel, YamlConfigSettingsSource


class RedisConfig(BaseModel):
    host: str = 'localhost'
    port: Annotated[int, Field(ge=1, le=65536)] = 6379

class TargetRedisConfig(BaseModel):
    host: str
    port: Annotated[int, Field(ge=1, le=65536)]
    buffer_length: Annotated[int, Field(ge=1)] = 100
    target_stream_maxlen: Annotated[int, Field(ge=1)] = 100
    tls: bool = False
    socket_timeout_s: Annotated[float, Field(ge=0)] = 5
    
class MappingConfig(BaseModel):
    source: str = None
    target: str = None

class RedisWriterConfig(BaseSettings):
    log_level: LogLevel = LogLevel.WARNING
    redis: RedisConfig = RedisConfig()
    target_redis: TargetRedisConfig
    remove_frame_data: bool = True
    prometheus_port: Annotated[int, Field(gt=1024, le=65536)] = 8000
    mapping_config: List[MappingConfig]

    model_config = SettingsConfigDict(env_nested_delimiter='__')

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings):
        return (init_settings, env_settings, YamlConfigSettingsSource(settings_cls), file_secret_settings)