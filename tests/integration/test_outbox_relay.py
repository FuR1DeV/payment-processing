"""Outbox relay on real PostgreSQL and RabbitMQ."""

import asyncio
import json
from datetime import UTC, datetime, timedelta

from aio_pika import DeliveryMode
from faststream.rabbit import RabbitBroker

from app.bootstrap import ApiContainer
from app.core.clock import Clock, utc_now
from app.messaging.broker import RabbitEventPublisher
from app.messaging.relay import OutboxRelay
from app.messaging.topology import Topology
from app.services.payments import RequeueStalePayments
from app.services.ports import EventPublisher
from tests.fakes import FakePublisher

from .helpers import execute, new_payment, one_row, scalar


class MovableClock:
    def __init__(self) -> None:
        self.now = datetime.now(UTC)

    def __call__(self) -> datetime:
        return self.now


def make_relay(
    api: ApiContainer, publisher: EventPublisher, clock: Clock = utc_now, batch_size: int = 10
) -> OutboxRelay:
    return OutboxRelay(
        uow_factory=api.db.uow_factory,
        publisher=publisher,
        batch_size=batch_size,
        retry_base_seconds=1,
        retry_max_seconds=60,
        clock=clock,
    )


async def test_event_reaches_the_queue_and_is_marked_published(
    api: ApiContainer, broker: RabbitBroker, topology: Topology
) -> None:
    payment_id = await new_payment(api)

    assert await make_relay(api, RabbitEventPublisher(broker, topology)).run_once() == 1

    queue = await broker.declare_queue(topology.new_payments)
    message = await queue.get(no_ack=True, timeout=5)
    assert json.loads(message.body)["payment_id"] == str(payment_id)
    assert message.message_id == await scalar("SELECT event_id::text FROM outbox")
    assert message.delivery_mode == DeliveryMode.PERSISTENT
    assert await scalar("SELECT count(*) FROM outbox WHERE published_at IS NOT NULL") == 1


async def test_published_event_is_not_sent_again(
    api: ApiContainer, broker: RabbitBroker, topology: Topology
) -> None:
    await new_payment(api)
    relay = make_relay(api, RabbitEventPublisher(broker, topology))
    assert await relay.run_once() == 1
    assert await relay.run_once() == 0


async def test_failed_publish_is_postponed(api: ApiContainer) -> None:
    await new_payment(api)
    clock = MovableClock()

    assert await make_relay(api, FakePublisher(fail=True), clock).run_once() == 0
    row = await one_row("SELECT * FROM outbox")
    assert row["published_at"] is None
    assert row["attempts"] == 1
    assert row["last_error"].startswith("ConnectionError")
    assert row["next_attempt_at"] == clock.now + timedelta(seconds=1)

    working = FakePublisher()
    assert await make_relay(api, working, clock).run_once() == 0  # not due yet
    clock.now += timedelta(seconds=2)
    assert await make_relay(api, working, clock).run_once() == 1


async def test_backoff_doubles_with_every_failure(api: ApiContainer) -> None:
    await new_payment(api)
    clock = MovableClock()
    broken = make_relay(api, FakePublisher(fail=True), clock)
    delays = []
    for _ in range(4):
        await broken.run_once()
        row = await one_row("SELECT * FROM outbox")
        delays.append((row["next_attempt_at"] - clock.now).total_seconds())
        clock.now = row["next_attempt_at"]
    assert delays == [1, 2, 4, 8]


async def test_unroutable_event_stays_in_the_outbox(
    api: ApiContainer, broker: RabbitBroker, topology: Topology
) -> None:
    """mandatory=True: no queue is bound for this routing key, so the broker returns the
    message; the row is not marked as published (otherwise the event would be lost)."""
    await new_payment(api)
    await execute("UPDATE outbox SET event_type = 'payment.nobody_listens'")

    assert await make_relay(api, RabbitEventPublisher(broker, topology)).run_once() == 0
    row = await one_row("SELECT * FROM outbox")
    assert row["published_at"] is None
    assert row["attempts"] == 1


async def test_one_bad_row_does_not_hold_up_the_rows_behind_it(api: ApiContainer) -> None:
    for _ in range(3):
        await new_payment(api)
    await execute("UPDATE outbox SET event_type = 'payment.nobody_listens' WHERE id = 1")
    publisher = FakePublisher(unroutable=frozenset({"payment.nobody_listens"}))

    assert await make_relay(api, publisher).run_once() == 2
    assert await scalar("SELECT attempts FROM outbox WHERE id = 1") == 1


async def test_broker_down_stops_the_batch_after_three_failures(api: ApiContainer) -> None:
    for _ in range(5):
        await new_payment(api)
    assert await make_relay(api, FakePublisher(fail=True)).run_once() == 0
    assert await scalar("SELECT count(*) FROM outbox WHERE attempts = 1") == 3


async def test_a_locked_row_is_skipped_not_waited_for(api: ApiContainer) -> None:
    """SKIP LOCKED: while one relay is busy with row 1, another takes row 2 at once
    instead of waiting for the first transaction to finish."""
    for _ in range(2):
        await new_payment(api)
    slow = asyncio.create_task(make_relay(api, FakePublisher(delay=1.0), batch_size=1).run_once())
    await asyncio.sleep(0.2)  # the slow relay holds row 1 now

    loop = asyncio.get_running_loop()
    started = loop.time()
    assert await make_relay(api, FakePublisher(), batch_size=1).run_once() == 1
    assert loop.time() - started < 0.5
    assert await slow == 1


def requeue_stale(api: ApiContainer, at: datetime, max_requeues: int = 3) -> RequeueStalePayments:
    def clock() -> datetime:
        return at

    return RequeueStalePayments(
        uow_factory=api.db.uow_factory,
        older_than=timedelta(minutes=5),
        max_requeues=max_requeues,
        batch_size=100,
        clock=clock,
    )


async def publish_all(at: datetime) -> None:
    await execute("UPDATE outbox SET published_at = :at WHERE published_at IS NULL", at=at)


async def test_stale_pending_payment_gets_a_new_event(api: ApiContainer) -> None:
    """Its message was consumed but the payment is still pending (the message went to the
    DLQ during a database blip, say): five minutes later a fresh event is added, once."""
    payment_id = await new_payment(api)
    now = datetime.now(UTC)
    await publish_all(now)

    assert await requeue_stale(api, now + timedelta(minutes=6))() == 1
    unpublished = "SELECT count(*) FROM outbox WHERE aggregate_id = :id AND published_at IS NULL"
    assert await scalar(unpublished, id=payment_id) == 1
    # The new event is still waiting for the relay: no second one.
    assert await requeue_stale(api, now + timedelta(minutes=7))() == 0


async def test_age_counts_from_publishing_not_from_creation(api: ApiContainer) -> None:
    """The broker was down for an hour: the event waited in the outbox and has only just
    been published. The payment is not stale, its message is on its way."""
    await new_payment(api)
    hour_ago = datetime.now(UTC) - timedelta(hours=1)
    await execute("UPDATE payments SET created_at = :t", t=hour_ago)
    await execute("UPDATE outbox SET created_at = :t", t=hour_ago)
    now = datetime.now(UTC)
    await publish_all(now)

    assert await requeue_stale(api, now + timedelta(minutes=1))() == 0


async def test_pauses_double_and_the_number_of_new_events_is_limited(api: ApiContainer) -> None:
    await new_payment(api)
    t = datetime.now(UTC)
    await publish_all(t)
    for wait in (5, 10, 20):  # minutes after the previous event was published
        assert await requeue_stale(api, t + timedelta(minutes=wait - 1))() == 0
        t += timedelta(minutes=wait)
        assert await requeue_stale(api, t)() == 1
        await publish_all(t)
    # Three fresh events were enough: stuck for good, it is left for people (and the logs).
    assert await requeue_stale(api, t + timedelta(days=1))() == 0
    assert await scalar("SELECT count(*) FROM outbox") == 4


async def test_two_sweeps_at_once_add_one_event_per_payment(api: ApiContainer) -> None:
    for _ in range(20):
        await new_payment(api)
    now = datetime.now(UTC)
    await publish_all(now)
    later = now + timedelta(minutes=6)

    added = await asyncio.gather(requeue_stale(api, later)(), requeue_stale(api, later)())

    assert sum(added) == 20
    assert await scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL") == 20


async def test_young_waiting_or_processed_payments_are_left_alone(api: ApiContainer) -> None:
    await new_payment(api)  # its event is not published yet
    now = datetime.now(UTC)
    assert await requeue_stale(api, now + timedelta(hours=1))() == 0

    await publish_all(now)
    assert await requeue_stale(api, now + timedelta(minutes=1))() == 0  # too young

    await execute("UPDATE payments SET status = 'succeeded', processed_at = now()")
    assert await requeue_stale(api, now + timedelta(hours=1))() == 0  # not pending any more


async def test_parallel_relays_publish_each_event_once(api: ApiContainer) -> None:
    """Three relays drain one table and no event is published twice."""
    for _ in range(30):
        await new_payment(api)
    publisher = FakePublisher(delay=0.005)
    relays = [make_relay(api, publisher, batch_size=4) for _ in range(3)]

    async def drain(relay: OutboxRelay) -> None:
        while await relay.run_once():
            pass

    await asyncio.gather(*(drain(r) for r in relays))

    assert len(publisher.published) == 30
    assert len(set(publisher.published)) == 30
    assert await scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL") == 0
