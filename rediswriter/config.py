from pydantic import BaseModel, conint, conlist
from pydantic_settings import BaseSettings, SettingsConfigDict
from visionlib.pipeline.settings import LogLevel, YamlConfigSettingsSource
from typing import List


class RedisConfig(BaseModel):
    host: str = 'localhost'
    port: conint(ge=1, le=65536) = 6379
    stream_ids: List[str] = ['stream1']
    input_stream_prefix: str = 'objecttracker'

class TargetRedisConfig(BaseModel):
    host: str
    port: conint(ge=1, le=65536)
    stream_key: str = 'output'
    buffer_length: conint(ge=1) = 1
    target_stream_maxlen: conint(ge=1) = 100

class RedisWriterConfig(BaseSettings):
    log_level: LogLevel = LogLevel.WARNING
    redis: RedisConfig = RedisConfig()
    target_redis: TargetRedisConfig

    model_config = SettingsConfigDict(env_nested_delimiter='__')

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings):
        return (init_settings, env_settings, YamlConfigSettingsSource(settings_cls), file_secret_settings)