import asyncio
import os
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import httpx
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.bootstrap import ApiContainer
from app.core.config import Settings
from app.domain.events import PAYMENT_CREATED, PaymentCreated
from app.domain.payment import Currency
from app.messaging.topology import Topology
from app.services.payments import CreatePaymentCommand

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+asyncpg://payments:payments@localhost:5432/payments_test"
)
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://payments:payments@localhost:5672/")
API_KEY = "test-api-key-0123456789"
WEBHOOK_SECRET = "test-webhook-secret-0123"


def make_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "database_url": DATABASE_URL,
        "rabbitmq_url": RABBITMQ_URL,
        "api_key": API_KEY,
        "webhook_secret": WEBHOOK_SECRET,
        # Every test gets its own exchanges and queues.
        "rabbitmq_namespace": f"test-{uuid4().hex[:12]}",
        "retry_base_delay_seconds": 0.1,
        "outbox_poll_interval_seconds": 0.05,
        "outbox_retry_base_seconds": 1.0,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)  # type: ignore[call-arg]


def payment_body(**overrides: Any) -> dict[str, Any]:
    return {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Loan repayment",
        "metadata": {"order_id": "A-1001", "client": {"id": 42}},
        "webhook_url": "http://client.test/webhook",
    } | overrides


async def create_payment(
    client: httpx.AsyncClient, key: str | None = None, **body: Any
) -> httpx.Response:
    return await client.post(
        "/api/v1/payments",
        json=payment_body(**body),
        headers={"X-API-Key": API_KEY, "Idempotency-Key": key or uuid4().hex},
    )


async def new_payment(api: ApiContainer, amount: str = "250.50") -> UUID:
    """A pending payment with its outbox event, created the way the API creates it."""
    result = await api.create_payment(
        CreatePaymentCommand(
            idempotency_key=uuid4().hex,
            amount=Decimal(amount),
            currency=Currency.USD,
            description="Loan repayment",
            webhook_url="http://client.test/webhook",
            metadata={"order_id": "A-7"},
        )
    )
    return result.payment.id


async def publish_created(
    broker: RabbitBroker, topology: Topology, payment_id: UUID, event_id: UUID | None = None
) -> None:
    """Publish a payment.created event the way the relay does, bypassing the outbox."""
    event = PaymentCreated(
        event_id=event_id or uuid4(), payment_id=payment_id, occurred_at=datetime.now(UTC)
    )
    await broker.publish(
        event.model_dump(mode="json"),
        exchange=topology.exchange,
        routing_key=PAYMENT_CREATED,
        message_id=str(event.event_id),
        persist=True,
    )


async def eventually(
    check: Callable[[], Awaitable[bool]], within: float = 10.0, message: str = ""
) -> None:
    deadline = asyncio.get_running_loop().time() + within
    while not await check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"not reached in {within} s: {message}")
        await asyncio.sleep(0.05)


async def scalar(sql: str, **params: Any) -> Any:
    engine = create_async_engine(DATABASE_URL)
    try:
        async with engine.connect() as conn:
            return (await conn.execute(text(sql), params)).scalar()
    finally:
        await engine.dispose()


async def one_row(sql: str, **params: Any) -> dict[str, Any]:
    engine = create_async_engine(DATABASE_URL)
    try:
        async with engine.connect() as conn:
            return dict((await conn.execute(text(sql), params)).mappings().one())
    finally:
        await engine.dispose()


async def execute(sql: str, **params: Any) -> None:
    engine = create_async_engine(DATABASE_URL)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(sql), params)
    finally:
        await engine.dispose()
