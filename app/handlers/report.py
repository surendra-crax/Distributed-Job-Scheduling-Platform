import asyncio
import hashlib
import time
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.handlers.base import BaseJobHandler, JobExecutionContext
from app.handlers.registry import register_handler


class ReportPayload(BaseModel):
    """Schema for simulated report generation."""

    report_type: Literal["sales_summary", "user_activity", "inventory_audit"]
    date_range_days: int = Field(default=30, ge=1, le=365)
    simulated_duration_ms: int = Field(
        default=50,
        ge=0,
        le=60000,
        description="Simulated generation time in milliseconds",
    )
    row_count: int = Field(default=1000, ge=1, le=1_000_000)


@register_handler
class ReportSimHandler(BaseJobHandler):
    """Generates synthetic reports and computes deterministic summary checksums."""

    job_type = "report.generate"
    payload_schema = ReportPayload

    async def execute(
        self,
        payload: ReportPayload,
        context: JobExecutionContext,
    ) -> dict[str, Any]:
        start = time.perf_counter()

        # Simulate work in chunks to support cancellation checking
        chunk_time_s = 0.05
        total_sleep_s = payload.simulated_duration_ms / 1000.0
        elapsed_sleep = 0.0

        while elapsed_sleep < total_sleep_s:
            sleep_step = min(chunk_time_s, total_sleep_s - elapsed_sleep)
            await asyncio.sleep(sleep_step)
            elapsed_sleep += sleep_step
            context.cancellation_token.raise_if_cancelled()

        # Deterministic checksum generation based on inputs
        hasher = hashlib.sha256()
        hasher.update(
            f"{payload.report_type}:{payload.date_range_days}:{payload.row_count}".encode()
        )
        report_hash = hasher.hexdigest()[:16]

        total_duration_ms = (time.perf_counter() - start) * 1000.0

        return {
            "report_type": payload.report_type,
            "date_range_days": payload.date_range_days,
            "rows_processed": payload.row_count,
            "report_checksum": report_hash,
            "duration_ms": round(total_duration_ms, 2),
            "generated_by_worker": context.worker_id,
        }
