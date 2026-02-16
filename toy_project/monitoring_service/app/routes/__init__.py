from .commands import router as commands_router
from .queries import router as queries_router
from .health import router as health_router
from .alerts import router as alerts_router

__all__ = ["commands_router", "queries_router", "health_router", "alerts_router"]
