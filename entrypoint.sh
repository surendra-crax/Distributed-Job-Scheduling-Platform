#!/usr/bin/env bash
set -e

echo "=========================================================="
echo "⚡ Starting Distributed Job Scheduling Platform"
echo "Mode: ${MODE:-all-in-one}"
echo "Environment: ${ENVIRONMENT:-production}"
echo "=========================================================="

# 1. Run Alembic Database Migrations
echo "🔄 Checking and applying database migrations..."
alembic upgrade head
echo "✅ Database schema up to date."

# 2. Execution Dispatch based on MODE / RUN_ALL_SERVICES
PORT="${PORT:-8000}"

if [ "${MODE}" = "api" ]; then
    echo "🚀 Starting FastAPI Web & Mission Control server on port ${PORT}..."
    exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT}"

elif [ "${MODE}" = "scheduler" ]; then
    echo "⏱️ Starting Scheduler Daemon..."
    exec python -m app.scheduler.scheduler

elif [ "${MODE}" = "worker" ]; then
    echo "⚙️ Starting Worker Process Pool..."
    exec python -m app.workers.worker

elif [ "${MODE}" = "recovery" ]; then
    echo "🛡️ Starting Crash Recovery Reaper..."
    exec python -m app.services.recovery_service

else
    # Default: All-in-one unified mode (Ideal for single-container & Render Web Service)
    echo "🌟 Launching in Unified Multi-Process Mode (API + Scheduler + Worker + Reaper)..."

    # Trap termination signals to gracefully stop background processes
    trap 'echo "🛑 Received termination signal. Shutting down..."; kill $(jobs -p); exit 0' SIGTERM SIGINT

    echo "  [1/4] Starting Scheduler Daemon in background..."
    python -m app.scheduler.scheduler &
    SCHED_PID=$!

    echo "  [2/4] Starting Worker Process Pool in background..."
    python -m app.workers.worker &
    WORKER_PID=$!

    echo "  [3/4] Starting Recovery Reaper in background..."
    python -m app.services.recovery_service &
    REAPER_PID=$!

    echo "  [4/4] Starting FastAPI Web & Mission Control server on port ${PORT}..."
    uvicorn app.main:app --host 0.0.0.0 --port "${PORT}" &
    API_PID=$!

    # Wait for the API process; if any process dies, keep others running or exit cleanly
    wait -n ${API_PID} ${WORKER_PID} ${SCHED_PID} ${REAPER_PID}
fi
