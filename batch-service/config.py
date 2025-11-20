"""Configuration for batch service"""

import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings"""

    # NATS configuration
    NATS_URL: str = os.getenv("NATS_URL", "nats://localhost:4222")

    # Service configuration
    SERVICE_NAME: str = "batch-service"
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Processing configuration
    DELAYED_UPDATE_DELAY_SECONDS: int = 300  # 5 minutes delay
    AGGREGATION_WINDOW_HOURS: int = 1
    COMPACTION_RETENTION_DAYS: int = 90

    # Database configuration (shard info)
    TOTAL_SHARDS: int = 4

    def get_shard_config(self, shard_id: int) -> dict:
        """Get database configuration for a specific shard"""
        return {
            "host": os.getenv(f"SHARD_{shard_id}_WRITE_HOST", f"shard-{shard_id}-write"),
            "port": int(os.getenv(f"SHARD_{shard_id}_WRITE_PORT", "5432")),
            "database": os.getenv(f"SHARD_{shard_id}_DB", f"shardstream_shard_{shard_id}"),
            "user": os.getenv(f"SHARD_{shard_id}_USER", "postgres"),
            "password": os.getenv(f"SHARD_{shard_id}_PASSWORD", "postgres"),
        }

    class Config:
        case_sensitive = True


settings = Settings()
