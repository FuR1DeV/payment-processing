from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import sqlalchemy as sa
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api.payments import router as payments_router
from app.bootstrap import ApiContainer, build_api
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.demo.router import router as demo_router

DEMO_DIR = Path(__file__).parent / "demo" / "static"


def create_app(container: ApiContainer | None = None) -> FastAPI:
    """Run: uvicorn app.main:create_app --factory. With a ready container (tests) the
    lifespan does not build or close anything.

    The payment endpoints do not talk to RabbitMQ at all: they write the payment and the
    outbox event to PostgreSQL, the relay publishes. Payments are accepted even while the
    broker is down. Only the demo helpers (/demo/api, off in production) look at the queues.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if container is not None:
            yield
            return
        settings = get_settings()
        configure_logging(settings.log_level)
        app.state.container = build_api(settings)
        try:
            yield
        finally:
            await app.state.container.close()

    settings = container.settings if container is not None else get_settings()
    # Swagger cannot send X-API-Key from a plain browser tab, so the docs stay open locally
    # and are switched off with DOCS_ENABLED=false anywhere real.
    docs = settings.docs_enabled
    app = FastAPI(
        title="Payment Processing",
        version="0.1.0",
        description="Asynchronous payment processing with an outbox, RabbitMQ and webhooks.",
        lifespan=lifespan,
        docs_url="/docs" if docs else None,
        redoc_url=None,
        openapi_url="/openapi.json" if docs else None,
    )
    if container is not None:
        app.state.container = container
    app.include_router(payments_router)
    if settings.demo_page_enabled:
        # The router first: the static mount at /demo would otherwise catch /demo/api too.
        app.include_router(demo_router)
        app.mount("/demo", StaticFiles(directory=DEMO_DIR, html=True), name="demo")

    @app.get("/health", tags=["service"])
    async def health(request: Request) -> JSONResponse:
        """No API key here: it is for Docker and orchestrator probes."""
        c: ApiContainer = request.app.state.container
        try:
            async with c.db.engine.connect() as conn:
                await conn.execute(sa.text("SELECT 1"))
        except Exception as exc:
            return JSONResponse(
                {"status": "degraded", "postgres": f"error: {type(exc).__name__}"},
                status_code=503,
            )
        return JSONResponse({"status": "ok", "postgres": "ok"})

    return app
