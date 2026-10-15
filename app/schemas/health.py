from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field


class DependencyCheck(BaseModel):
    status: Literal["healthy", "unhealthy"]
    latency_ms: float | None = None
    error: str | None = None


class HealthLiveResponse(BaseModel):
    status: Literal["alive"] = "alive"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    app_name: str
    environment: str


class HealthReadyResponse(BaseModel):
    status: Literal["ready", "unhealthy"]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    checks: dict[str, DependencyCheck]
