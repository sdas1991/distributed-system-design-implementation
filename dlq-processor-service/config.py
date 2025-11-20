"""Configuration for DLQ Processor Service"""

import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """DLQ Processor settings"""

    # NATS configuration
    NATS_URL: str = os.getenv("NATS_URL", "nats://localhost:4222")

    # Service configuration
    SERVICE_NAME: str = "dlq-processor"
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # DLQ configuration
    MAX_RETRIES: int = 3
    RETRY_BACKOFF_SECONDS: list = [10, 30, 90]  # Exponential backoff

    # Database configuration (for storing permanently failed messages)
    DB_HOST: str = os.getenv("DLQ_DB_HOST", "shard-0-write")
    DB_PORT: int = int(os.getenv("DLQ_DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DLQ_DB_NAME", "shardstream_shard_0")
    DB_USER: str = os.getenv("DLQ_DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DLQ_DB_PASSWORD", "postgres")

    class Config:
        case_sensitive = True


settings = Settings()
