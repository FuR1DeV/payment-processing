"""What the demo page needs besides the public API: the outbox events of a payment, the
queues, the demo webhook receiver and a DLQ replay. Built only when DEMO_PAGE_ENABLED is on.

This is the only place where the API process connects to RabbitMQ, and only lazily, on the
first request that needs it. The payment endpoints themselves never touch the broker.
"""

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import httpx
from faststream.rabbit import RabbitBroker

from app.core.config import Settings
from app.db.models import OutboxEvent, Payment
from app.messaging.broker import create_broker
from app.messaging.dlq import replay_dead_letters
from app.messaging.topology import Topology, declare
from app.services.ports import UnitOfWorkFactory


class DemoBackendError(Exception):
    """The receiver or RabbitMQ did not answer; the page shows it and asks again later."""


@dataclass(frozen=True)
class DbView:
    payments: Sequence[Payment]
    payments_by_status: dict[str, int]
    outbox: Sequence[OutboxEvent]
    outbox_total: int
    outbox_unpublished: int


@dataclass(frozen=True)
class QueueState:
    name: str
    role: str  # main | retry | postpone | dlq
    delay_ms: int | None
    messages: int  # ready to be delivered (not counting the ones being handled right now)
    consumers: int


class DemoTools:
    def __init__(
        self,
        settings: Settings,
        uow_factory: UnitOfWorkFactory,
        *,
        receiver_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._settings = settings
        self._uow_factory = uow_factory
        self._topology = Topology.from_settings(settings)
        self._receiver = httpx.AsyncClient(
            base_url=settings.demo_receiver_url, timeout=5.0, transport=receiver_transport
        )
        self._broker: RabbitBroker | None = None
        self._broker_lock = asyncio.Lock()

    async def outbox_events(self, payment_id: UUID) -> Sequence[OutboxEvent]:
        async with self._uow_factory() as uow:
            return await uow.outbox.list_for(payment_id)

    async def tables(self, limit: int) -> DbView:
        """The latest rows of both tables and their counts, as they are in PostgreSQL."""
        async with self._uow_factory() as uow:
            payments = await uow.payments.recent(limit)
            by_status = await uow.payments.count_by_status()
            outbox = await uow.outbox.recent(limit)
            outbox_total, outbox_unpublished = await uow.outbox.counts()
        return DbView(
            payments=payments,
            payments_by_status=by_status,
            outbox=outbox,
            outbox_total=outbox_total,
            outbox_unpublished=outbox_unpublished,
        )

    async def receiver(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, json: Any = None
    ) -> Any:
        try:
            response = await self._receiver.request(method, path, params=params, json=json)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise DemoBackendError(f"webhook receiver: {type(exc).__name__}") from exc
        return response.json()

    async def queues(self) -> list[QueueState]:
        broker = await self._connected_broker()
        t = self._topology
        specs = [("main", None, t.new_payments)]
        for delay in t.delays_ms:
            role = "retry" if delay in t.retry_delays_ms else "postpone"
            specs.append((role, delay, t.retry_queue(delay)))
        specs.append(("dlq", None, t.dead_letter_queue))
        states = []
        try:
            for role, delay, spec in specs:
                queue = await broker.declare_queue(spec)
                result = await queue.declare()  # a fresh declare returns the current counts
                states.append(
                    QueueState(
                        name=spec.name,
                        role=role,
                        delay_ms=delay,
                        messages=result.message_count or 0,
                        consumers=result.consumer_count or 0,
                    )
                )
        except Exception as exc:
            raise DemoBackendError(f"RabbitMQ: {type(exc).__name__}") from exc
        return states

    async def replay_dlq(self) -> int:
        broker = await self._connected_broker()
        try:
            return await replay_dead_letters(broker, self._topology)
        except Exception as exc:
            raise DemoBackendError(f"RabbitMQ: {type(exc).__name__}") from exc

    async def _connected_broker(self) -> RabbitBroker:
        async with self._broker_lock:
            if self._broker is None:
                broker = create_broker(self._settings)
                try:
                    await broker.connect()
                    await declare(broker, self._topology)
                except Exception as exc:
                    await broker.stop()
                    raise DemoBackendError(f"RabbitMQ: {type(exc).__name__}") from exc
                self._broker = broker
            return self._broker

    async def close(self) -> None:
        await self._receiver.aclose()
        if self._broker is not None:
            await self._broker.stop()
