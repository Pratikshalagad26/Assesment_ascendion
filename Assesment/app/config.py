from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_database: str = "cohort_insights"

    redis_url: str = "redis://localhost:6379/0"

    cache_ttl_seconds: int = 86400
    max_active_pipelines: int = 3
    # "open" = allow requests if Redis is down; "closed" = reject with 503
    rate_limit_on_redis_failure: str = "open"

    processing_delay_min: float = 10.0
    processing_delay_max: float = 20.0
    enriching_delay_min: float = 5.0
    enriching_delay_max: float = 15.0
    stage_failure_rate: float = 0.1

    worker_poll_interval_seconds: float = 1.0

    log_level: str = "INFO"
    app_host: str = "0.0.0.0"
    app_port: int = 8000


@lru_cache
def get_settings() -> Settings:
    return Settings()
