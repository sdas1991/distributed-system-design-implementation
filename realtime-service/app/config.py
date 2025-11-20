"""Configuration settings for realtime service"""

import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings"""

    # NATS configuration
    NATS_URL: str = os.getenv("NATS_URL", "nats://localhost:4222")

    # Service configuration
    SERVICE_NAME: str = "realtime-service"
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # WebSocket configuration
    WS_HEARTBEAT_INTERVAL: int = 30  # seconds
    WS_MAX_CONNECTIONS_PER_CUSTOMER: int = 10

    class Config:
        case_sensitive = True


settings = Settings()
