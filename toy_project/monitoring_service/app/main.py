import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from cw_common.observability import init_observability, get_logger, shutdown_tracing

from app.database import init_db
from app.routes import queries_router, health_router, alerts_router

# Bootstrap logging + tracing + service-info in one call
init_observability("monitoring-api", "0.3.0")

logger = get_logger("monitoring-api")

_kafka_thread = None
_kafka_stop = None
_alert_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _kafka_thread, _kafka_stop, _alert_task

    init_db()
    logger.info("Database initialized")

    # Start Kafka consumer thread if configured
    kafka_enabled = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if kafka_enabled:
        from app.consumer import start_consumer
        _kafka_thread, _kafka_stop = start_consumer()
        logger.info("Kafka consumer started")

    # Start background alert scheduler
    from app.scheduler import alert_loop
    _alert_task = asyncio.create_task(alert_loop())
    logger.info("Background alert scheduler started")

    yield

    # Shutdown: cancel alert scheduler
    if _alert_task:
        _alert_task.cancel()
        try:
            await _alert_task
        except asyncio.CancelledError:
            pass

    if _kafka_stop:
        _kafka_stop.set()
        logger.info("Kafka consumer stopping...")
    
    # Flush remaining traces before shutdown
    shutdown_tracing()


app = FastAPI(
    title="Transaction Monitoring Service",
    version="0.3.0",
    lifespan=lifespan,
)

app.include_router(queries_router)
app.include_router(health_router)
app.include_router(alerts_router)

# Initialize telemetry at module level (before requests start)
try:
    from app import telemetry
    telemetry.init(app)
except Exception as e:
    logger.warning(f"Telemetry init skipped: {e}")
