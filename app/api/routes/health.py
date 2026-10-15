import time

from fastapi import APIRouter, Response, status

from app.core.config import get_settings
from app.db.session import check_database_connection
from app.queue.redis_queue import check_redis_connection
from app.schemas.health import DependencyCheck, HealthLiveResponse, HealthReadyResponse

router = APIRouter(tags=["Health & Diagnostics"])


@router.get(
    "/health/live",
    response_model=HealthLiveResponse,
    summary="Liveness probe",
    description="Returns 200 if the API service process is alive and responding.",
)
async def health_live() -> HealthLiveResponse:
    settings = get_settings()
    return HealthLiveResponse(
        status="alive",
        app_name=settings.APP_NAME,
        environment=settings.ENVIRONMENT,
    )


@router.get(
    "/health/ready",
    response_model=HealthReadyResponse,
    summary="Readiness probe",
    description=(
        "Validates connectivity to PostgreSQL and Redis. "
        "Returns 503 if any required dependency fails."
    ),
)
async def health_ready(response: Response) -> HealthReadyResponse:
    # Check PostgreSQL
    db_start = time.perf_counter()
    db_ok, db_err = await check_database_connection()
    db_latency = (time.perf_counter() - db_start) * 1000.0

    # Check Redis
    redis_ok, redis_latency, redis_err = await check_redis_connection()

    all_healthy = db_ok and redis_ok
    if not all_healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthReadyResponse(
        status="ready" if all_healthy else "unhealthy",
        checks={
            "database": DependencyCheck(
                status="healthy" if db_ok else "unhealthy",
                latency_ms=round(db_latency, 2) if db_ok else None,
                error=db_err,
            ),
            "redis": DependencyCheck(
                status="healthy" if redis_ok else "unhealthy",
                latency_ms=redis_latency,
                error=redis_err,
            ),
        },
    )
