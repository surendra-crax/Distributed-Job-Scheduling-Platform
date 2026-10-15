import math
import time
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.exceptions.errors import PermanentJobError
from app.handlers.base import BaseJobHandler, JobExecutionContext
from app.handlers.registry import register_handler


class CalculationPayload(BaseModel):
    """Schema for calculation jobs."""

    operation: Literal["sum", "multiply", "factorial", "fibonacci"]
    operands: list[int] = Field(default_factory=list, description="Operands for sum/multiply")
    n: int | None = Field(default=None, description="Input value for factorial or fibonacci")


@register_handler
class CalculationHandler(BaseJobHandler):
    """Executes deterministic mathematical computations."""

    job_type = "math.calculate"
    payload_schema = CalculationPayload

    async def execute(
        self,
        payload: CalculationPayload,
        context: JobExecutionContext,
    ) -> dict[str, Any]:
        start = time.perf_counter()
        op = payload.operation

        if op == "sum":
            computed = sum(payload.operands)
        elif op == "multiply":
            computed = 1
            for x in payload.operands:
                computed *= x
        elif op == "factorial":
            if payload.n is None or payload.n < 0:
                raise PermanentJobError("Factorial requires non-negative integer 'n'")
            if payload.n > 1000:
                raise PermanentJobError("Factorial input 'n' exceeds maximum allowed limit (1000)")
            computed = math.factorial(payload.n)
        elif op == "fibonacci":
            if payload.n is None or payload.n < 0:
                raise PermanentJobError("Fibonacci requires non-negative integer 'n'")
            if payload.n > 10000:
                raise PermanentJobError("Fibonacci input 'n' exceeds limit (10000)")

            a, b = 0, 1
            for _ in range(payload.n):
                a, b = b, a + b
            computed = a
        else:
            raise PermanentJobError(f"Unsupported calculation operation: '{op}'")

        duration_ms = (time.perf_counter() - start) * 1000.0

        return {
            "operation": op,
            "result": computed
            if isinstance(computed, int) and computed < 10**18
            else str(computed),
            "duration_ms": round(duration_ms, 3),
            "operands_count": len(payload.operands) if payload.operands else 1,
        }
