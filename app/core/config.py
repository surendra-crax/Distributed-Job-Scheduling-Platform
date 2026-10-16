from functools import lru_cache
from typing import Literal

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables and .env files."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # General
    ENVIRONMENT: Literal["development", "testing", "production"] = "development"
    APP_NAME: str = "Distributed Job Scheduling Platform"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"

    # API Server
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    # PostgreSQL Connection Parameters
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "job_scheduler"
    DATABASE_URL: str | None = None
    DATABASE_SYNC_URL: str | None = None

    # Connection Pool Settings
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 10
    DB_POOL_TIMEOUT_SECONDS: float = 30.0
    DB_POOL_RECYCLE_SECONDS: int = 1800

    @computed_field  # type: ignore[prop-decorator]
    @property
    def async_database_url(self) -> str:
        if self.DATABASE_URL:
            url = self.DATABASE_URL
            if url.startswith("postgres://"):
                url = url.replace("postgres://", "postgresql+asyncpg://", 1)
            elif url.startswith("postgresql://") and not url.startswith("postgresql+asyncpg://"):
                url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
            return url
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def sync_database_url(self) -> str:
        if self.DATABASE_SYNC_URL:
            url = self.DATABASE_SYNC_URL
        elif self.DATABASE_URL:
            url = self.DATABASE_URL
        else:
            return (
                f"postgresql+psycopg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
                f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
            )
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+psycopg://", 1)
        elif url.startswith("postgresql+asyncpg://"):
            url = url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
        elif url.startswith("postgresql://") and not url.startswith("postgresql+psycopg://"):
            url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        return url

    # Redis Parameters
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str | None = None
    REDIS_URL: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def resolved_redis_url(self) -> str:
        if self.REDIS_URL:
            return self.REDIS_URL
        auth = f":{self.REDIS_PASSWORD}@" if self.REDIS_PASSWORD else ""
        return f"redis://{auth}{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    # Queue Names
    REDIS_JOB_QUEUE_KEY: str = "scheduler:jobs:ready"
    REDIS_CANCELLATIONS_CHANNEL_PREFIX: str = "scheduler:cancellations:"

    # Scheduler Settings
    SCHEDULER_POLL_INTERVAL_SECONDS: float = 1.0
    SCHEDULER_BATCH_SIZE: int = 50
    SCHEDULER_LOCK_TIMEOUT_SECONDS: float = 10.0

    # Worker Settings
    WORKER_CONCURRENCY: int = 4
    WORKER_HEARTBEAT_INTERVAL_SECONDS: float = 15.0
    WORKER_DEFAULT_TIMEOUT_SECONDS: int = 300
    WORKER_QUEUE_WAIT_TIMEOUT_SECONDS: float = 2.0

    # Recovery & Reliability Settings
    RECOVERY_POLL_INTERVAL_SECONDS: float = 10.0
    RECOVERY_LEASE_GRACE_PERIOD_SECONDS: float = 10.0
    RECONCILIATION_INTERVAL_SECONDS: float = 30.0
    DEFAULT_MAX_ATTEMPTS: int = 3
    RETRY_BASE_DELAY_SECONDS: float = 2.0
    RETRY_MAX_DELAY_SECONDS: float = 300.0
    RETRY_JITTER: bool = True

    # Security & API Limits
    API_MAX_PAGE_SIZE: int = 100
    API_DEFAULT_PAGE_SIZE: int = 20
    MAX_PAYLOAD_BYTES: int = 1_048_576  # 1 MB


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached settings singleton."""
    return Settings()
