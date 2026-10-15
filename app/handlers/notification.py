import asyncio
import random
import time
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.exceptions.errors import PermanentJobError, RetryableJobError
from app.handlers.base import BaseJobHandler, JobExecutionContext
from app.handlers.registry import register_handler


class NotificationPayload(BaseModel):
    """Schema for simulated notification delivery."""

    recipient: str = Field(min_length=3, description="Target recipient address or number")
    channel: Literal["email", "sms", "push"]
    template: str = Field(min_length=1, description="Message body or template identifier")
    simulate_failure_rate: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Probability (0.0 - 1.0) of simulating a transient network outage",
    )
    simulated_delay_seconds: float = Field(
        default=0.0,
        ge=0.0,
        le=10.0,
        description="Simulated network transport latency",
    )


@register_handler
class NotificationSimHandler(BaseJobHandler):
    """Simulates multi-channel notification dispatch with configurable latency and transient faults."""

    job_type = "notification.simulate"
    payload_schema = NotificationPayload

    async def execute(
        self,
        payload: NotificationPayload,
        context: JobExecutionContext,
    ) -> dict[str, Any]:
        start = time.perf_counter()

        # Input sanity checks
        if payload.channel == "email" and "@" not in payload.recipient:
            raise PermanentJobError(f"Invalid email recipient: '{payload.recipient}'")

        if payload.simulated_delay_seconds > 0:
            await asyncio.sleep(payload.simulated_delay_seconds)
            context.cancellation_token.raise_if_cancelled()

        # Simulated transient network failure for testing retry policies
        if payload.simulate_failure_rate > 0.0:
            if random.random() < payload.simulate_failure_rate:
                raise RetryableJobError(
                    f"Transient network gateway error while contacting {payload.channel} upstream",
                    details={"attempt": context.attempt_number, "channel": payload.channel},
                )

        duration_ms = (time.perf_counter() - start) * 1000.0

        return {
            "channel": payload.channel,
            "recipient": payload.recipient,
            "status": "delivered",
            "message_id": f"msg_{context.job_id}_{context.attempt_number}",
            "duration_ms": round(duration_ms, 2),
        }
