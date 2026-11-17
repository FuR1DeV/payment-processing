"""Integration tests run against real PostgreSQL and RabbitMQ (docker compose or CI services)."""

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

import httpx
import pytest
from alembic import command
from alembic.config import Config
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.bootstrap import ApiContainer, ProcessorContainer, build_api, build_processor
from app.core.config import Settings
from app.main import create_app
from app.messaging.broker import create_broker
from app.messaging.consumer import PaymentEventHandler, subscribe
from app.messaging.topology import Topology, declare, retry_policy
from app.services.processing import ProcessingReport
from tests.fakes import FakeGateway, FakeWebhooks

from .helpers import DATABASE_URL, make_settings


@pytest.fixture(scope="session", autouse=True)
def _migrate() -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", DATABASE_URL)
    command.upgrade(config, "head")


@pytest.fixture(autouse=True)
async def _clean() -> None:
    engine = create_async_engine(DATABASE_URL)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox RESTART IDENTITY"))
    await engine.dispose()


@pytest.fixture
def settings() -> Settings:
    return make_settings()


@pytest.fixture
async def api(settings: Settings) -> AsyncIterator[ApiContainer]:
    container = build_api(settings)
    yield container
    await container.close()


@pytest.fixture
async def client(api: ApiContainer) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=create_app(api))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
def topology(settings: Settings) -> Topology:
    return Topology.from_settings(settings)


@pytest.fixture
async def broker(settings: Settings, topology: Topology) -> AsyncIterator[RabbitBroker]:
    """A connected broker with this test's own exchanges and queues; all removed afterwards."""
    broker = create_broker(settings)
    await broker.connect()
    await declare(broker, topology)
    yield broker
    queues = [
        topology.new_payments,
        topology.dead_letter_queue,
        *(topology.retry_queue(d) for d in topology.delays_ms),
    ]
    for queue in queues:
        await (await broker.declare_queue(queue)).delete(if_unused=False, if_empty=False)
    for exchange in (topology.exchange, topology.retry_exchange, topology.dead_letter_exchange):
        await (await broker.declare_exchange(exchange)).delete(if_unused=False)
    await broker.stop()


@dataclass
class RunningConsumer:
    processor: ProcessorContainer
    reports: list[ProcessingReport]  # one per successfully handled delivery


type StartConsumer = Callable[[FakeGateway, FakeWebhooks], Awaitable[RunningConsumer]]


@pytest.fixture
async def start_consumer(
    settings: Settings, topology: Topology, broker: RabbitBroker
) -> AsyncIterator[StartConsumer]:
    """Starts a consumer the way production does: its own broker connection, the same
    handler, the circuit breaker around the (fake) webhook sender."""
    started: list[tuple[RabbitBroker, ProcessorContainer]] = []

    async def start(gateway: FakeGateway, webhooks: FakeWebhooks) -> RunningConsumer:
        processor = build_processor(settings, gateway=gateway, webhooks=webhooks)
        consumer = RunningConsumer(processor=processor, reports=[])

        async def process_and_record(payment_id: UUID) -> ProcessingReport:
            report = await processor.process_payment(payment_id)
            consumer.reports.append(report)
            return report

        consumer_broker = create_broker(settings)
        handler = PaymentEventHandler(
            process_payment=process_and_record,
            broker=consumer_broker,
            topology=topology,
            policy=retry_policy(settings),
            max_postponements=settings.webhook_max_postponements,
        )
        subscribe(consumer_broker, topology, handler)
        await consumer_broker.start()
        started.append((consumer_broker, processor))
        return consumer

    yield start
    for consumer_broker, processor in started:
        await consumer_broker.stop()
        await processor.close()
