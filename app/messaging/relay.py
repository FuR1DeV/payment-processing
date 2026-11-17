"""Outbox relay: moves events from the outbox table to RabbitMQ, and re-sends events for
payments that have been pending for too long.

Run: python -m app.messaging.relay
"""

import asyncio
import contextlib
import logging
import signal
from collections.abc import Awaitable, Callable
from datetime import timedelta

from app.bootstrap import build_database
from app.core.clock import Clock, utc_now
from app.core.config import get_settings
from app.core.logging import configure_logging, log_context
from app.db.models import OutboxEvent
from app.db.repositories import UnitOfWork
from app.services.payments import RequeueStalePayments
from app.services.ports import EventPublisher, UnitOfWorkFactory

from .broker import RabbitEventPublisher, create_broker
from .topology import Topology, declare

logger = logging.getLogger("payments.relay")

# 2**30 seconds is already decades; a larger exponent only risks an OverflowError.
_MAX_BACKOFF_EXPONENT = 30
_STOP_AFTER_FAILURES_IN_A_ROW = 3


def backoff_delay(attempts: int, base: float, cap: float) -> float:
    """base, 2*base, 4*base, ... never more than cap, for any number of attempts."""
    return min(base * 2 ** min(attempts, _MAX_BACKOFF_EXPONENT), cap)


class OutboxRelay:
    """At-least-once delivery from the outbox to the broker.

    One batch is one transaction: lock unpublished rows (SKIP LOCKED), publish each with a
    broker confirm, mark the published ones, commit. If the process dies after publishing
    but before the commit, the rows stay unpublished and go out again: a duplicate, never a
    loss. The consumer is idempotent for exactly this reason.
    """

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        publisher: EventPublisher,
        batch_size: int,
        retry_base_seconds: float,
        retry_max_seconds: float,
        clock: Clock = utc_now,
    ) -> None:
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._retry_base = retry_base_seconds
        self._retry_max = retry_max_seconds
        self._clock = clock

    async def run_once(self) -> int:
        """Publish one batch. Returns how many events went out."""
        published: list[int] = []
        failures_in_a_row = 0
        async with self._uow_factory() as uow:
            batch = await uow.outbox.claim_batch(self._clock(), self._batch_size)
            for event in batch:
                with log_context(event_id=str(event.event_id), payment_id=str(event.aggregate_id)):
                    try:
                        await self._publisher.publish(event)
                    except Exception as exc:
                        await self._postpone(uow, event, exc)
                        failures_in_a_row += 1
                        # One bad row (an event nobody is bound to, say) must not hold up the
                        # rows behind it. Several failures in a row mean the broker itself is
                        # unreachable: stop, the rest are picked up after this commit.
                        if failures_in_a_row >= _STOP_AFTER_FAILURES_IN_A_ROW:
                            break
                        continue
                failures_in_a_row = 0
                published.append(event.id)
            await uow.outbox.mark_published(published, self._clock())
            await uow.commit()
        return len(published)

    async def _postpone(self, uow: UnitOfWork, event: OutboxEvent, exc: Exception) -> None:
        delay = backoff_delay(event.attempts, self._retry_base, self._retry_max)
        error = f"{type(exc).__name__}: {exc}"
        logger.warning("outbox_publish_failed", extra={"error": error, "retry_in_seconds": delay})
        await uow.outbox.mark_failed(event.id, error, self._clock() + timedelta(seconds=delay))

    async def run(self, stop: asyncio.Event, poll_interval: float) -> None:
        while not stop.is_set():
            try:
                sent = await self.run_once()
            except Exception:
                # E.g. PostgreSQL is down. Nothing is lost: the rows wait in the table.
                logger.exception("outbox_relay_error")
                sent = 0
            if sent:
                logger.info("outbox_published", extra={"count": sent})
            if sent < self._batch_size:  # a full batch means more may be waiting: no pause
                await _sleep(stop, poll_interval)


async def every(
    interval: float, job: Callable[[], Awaitable[int]], stop: asyncio.Event, name: str
) -> None:
    while not stop.is_set():
        try:
            if count := await job():
                logger.warning(name, extra={"count": count})
        except Exception:
            logger.exception(f"{name}_error")
        await _sleep(stop, interval)


async def _sleep(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), seconds)


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    db = build_database(settings)
    broker = create_broker(settings)
    topology = Topology.from_settings(settings)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    await broker.connect()
    try:
        await declare(broker, topology)
        relay = OutboxRelay(
            uow_factory=db.uow_factory,
            publisher=RabbitEventPublisher(broker, topology),
            batch_size=settings.outbox_batch_size,
            retry_base_seconds=settings.outbox_retry_base_seconds,
            retry_max_seconds=settings.outbox_retry_max_seconds,
        )
        requeue_stale = RequeueStalePayments(
            uow_factory=db.uow_factory,
            older_than=timedelta(seconds=settings.stale_pending_after_seconds),
            max_requeues=settings.stale_pending_max_requeues,
            batch_size=settings.outbox_batch_size,
        )
        logger.info("outbox_relay_started")
        await asyncio.gather(
            relay.run(stop, settings.outbox_poll_interval_seconds),
            every(settings.stale_check_interval_seconds, requeue_stale, stop, "stale_requeued"),
        )
    finally:
        await broker.stop()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
