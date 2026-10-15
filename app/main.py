from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import health, jobs, metrics
from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import close_db_engine
from app.queue.redis_queue import close_redis_client

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manages application startup and graceful shutdown."""
    configure_logging()
    settings = get_settings()
    logger.info(
        "api_startup",
        app_name=settings.APP_NAME,
        environment=settings.ENVIRONMENT,
        db_host=settings.POSTGRES_HOST,
        redis_host=settings.REDIS_HOST,
    )
    yield
    logger.info("api_shutdown_commencing")
    await close_db_engine()
    await close_redis_client()
    logger.info("api_shutdown_completed")


def create_application() -> FastAPI:
    """Builds and configures the FastAPI application instance."""
    settings = get_settings()

    app = FastAPI(
        title=settings.APP_NAME,
        version="0.1.0",
        description=(
            "Production-grade Distributed Job Scheduling Platform with durable PostgreSQL state, "
            "safe worker claiming with fencing tokens, dynamic priority scheduling, "
            "and crash recovery."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # CORS configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Global Exception Handlers
    @app.exception_handler(Exception)
    async def global_unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error(
            "unhandled_server_error",
            path=request.url.path,
            method=request.method,
            error=str(exc),
            exc_info=True,
        )
        # Avoid leaking internal stack traces to clients in production
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "InternalServerError",
                "message": "An unexpected error occurred while processing the request.",
            },
        )

    # Register Routers
    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(metrics.router)

    @app.get("/", include_in_schema=False)
    async def root() -> dict[str, str]:
        return {
            "name": settings.APP_NAME,
            "version": "0.1.0",
            "docs": "/docs",
            "health_live": "/health/live",
            "health_ready": "/health/ready",
        }

    return app


app = create_application()
