import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from cw_common.observability import init_observability, get_logger, shutdown_tracing

from app.database import init_db
from app.routes import commands_router, queries_router, health_router, alerts_router

# Bootstrap logging + tracing + service-info in one call
init_observability("monitoring-api", "0.3.0")

logger = get_logger("monitoring-api")

_kafka_thread = None
_kafka_stop = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _kafka_thread, _kafka_stop

    init_db()
    logger.info("Database initialized")

    # Start Kafka consumer thread if configured
    kafka_enabled = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if kafka_enabled:
        from app.consumer import start_consumer
        _kafka_thread, _kafka_stop = start_consumer()
        logger.info("Kafka consumer started")

    yield

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

# Register CQRS routers
app.include_router(commands_router)
app.include_router(queries_router)
app.include_router(health_router)
app.include_router(alerts_router)

# Initialize telemetry at module level (before requests start)
try:
    from app import telemetry
    telemetry.init(app)
except Exception as e:
    logger.warning(f"Telemetry init skipped: {e}")
