"""Composition root: builds what each process (api, relay, consumer) needs."""

from dataclasses import dataclass

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.config import Settings
from app.core.db import create_engine, create_session_factory
from app.db.repositories import UnitOfWork
from app.demo.tools import DemoTools
from app.integrations.circuit_breaker import BreakerWebhookSender, CircuitBreaker
from app.integrations.gateway import EmulatedGateway
from app.integrations.webhooks import HttpWebhookSender, create_http_client
from app.services.payments import CreatePayment, PaymentQueries
from app.services.ports import PaymentGateway, UnitOfWorkFactory, WebhookSender
from app.services.processing import ProcessPayment


@dataclass
class Database:
    engine: AsyncEngine
    uow_factory: UnitOfWorkFactory

    async def close(self) -> None:
        await self.engine.dispose()


def build_database(settings: Settings) -> Database:
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    return Database(engine=engine, uow_factory=lambda: UnitOfWork(session_factory))


@dataclass
class ApiContainer:
    settings: Settings
    db: Database
    create_payment: CreatePayment
    queries: PaymentQueries
    demo: DemoTools | None = None  # only with DEMO_PAGE_ENABLED

    async def close(self) -> None:
        if self.demo is not None:
            await self.demo.close()
        await self.db.close()


def build_api(
    settings: Settings, *, receiver_transport: httpx.AsyncBaseTransport | None = None
) -> ApiContainer:
    """Tests pass a transport that reaches the demo webhook receiver in process."""
    db = build_database(settings)
    demo = None
    if settings.demo_page_enabled:
        demo = DemoTools(settings, db.uow_factory, receiver_transport=receiver_transport)
    return ApiContainer(
        settings=settings,
        db=db,
        create_payment=CreatePayment(uow_factory=db.uow_factory),
        queries=PaymentQueries(uow_factory=db.uow_factory),
        demo=demo,
    )


@dataclass
class ProcessorContainer:
    db: Database
    http: httpx.AsyncClient | None
    breaker: CircuitBreaker
    process_payment: ProcessPayment

    async def close(self) -> None:
        if self.http is not None:
            await self.http.aclose()
        await self.db.close()


def build_processor(
    settings: Settings,
    *,
    gateway: PaymentGateway | None = None,
    webhooks: WebhookSender | None = None,
) -> ProcessorContainer:
    """Tests pass their own gateway and webhook sender; the breaker wraps either one."""
    db = build_database(settings)
    http = None
    if webhooks is None:
        http = create_http_client(settings.webhook_timeout_seconds)
        webhooks = HttpWebhookSender(
            http, settings.webhook_secret, timeout_seconds=settings.webhook_timeout_seconds
        )
    breaker = CircuitBreaker(
        failure_threshold=settings.webhook_breaker_failures,
        open_seconds=settings.webhook_breaker_open_seconds,
    )
    if gateway is None:
        gateway = EmulatedGateway(
            min_delay=settings.gateway_min_delay_seconds,
            max_delay=settings.gateway_max_delay_seconds,
            success_rate=settings.gateway_success_rate,
        )
    return ProcessorContainer(
        db=db,
        http=http,
        breaker=breaker,
        process_payment=ProcessPayment(
            uow_factory=db.uow_factory,
            gateway=gateway,
            webhooks=BreakerWebhookSender(webhooks, breaker),
        ),
    )
