"""The consumer on real RabbitMQ and PostgreSQL: retries through the delay queues, the DLQ,
redeliveries. Delays are shortened to 0.1 s and 0.2 s by the test settings."""

import json
from uuid import uuid4

from faststream.rabbit import RabbitBroker

from app.bootstrap import ApiContainer
from app.domain.events import PAYMENT_CREATED
from app.domain.payment import PaymentStatus
from app.messaging.broker import RabbitEventPublisher
from app.messaging.dlq import dead_letter_count, replay_dead_letters
from app.messaging.relay import OutboxRelay
from app.messaging.retry import ATTEMPT_HEADER, LAST_ERROR_HEADER
from app.messaging.topology import Topology
from app.services.processing import ProcessingReport
from tests.fakes import FakeGateway, FakeWebhooks

from .conftest import StartConsumer
from .helpers import eventually, new_payment, publish_created


async def dead_letters(broker: RabbitBroker, topology: Topology) -> list[object]:
    queue = await broker.declare_queue(topology.dead_letter_queue)
    messages = []
    while (message := await queue.get(no_ack=True, fail=False)) is not None:
        messages.append(message)
    return messages


async def test_full_pipeline_from_api_to_webhook(
    api: ApiContainer, broker: RabbitBroker, topology: Topology, start_consumer: StartConsumer
) -> None:
    """API transaction -> outbox -> relay -> payments.new -> consumer -> webhook."""
    gateway, webhooks = FakeGateway(), FakeWebhooks()
    await start_consumer(gateway, webhooks)
    payment_id = await new_payment(api)
    relay = OutboxRelay(
        uow_factory=api.db.uow_factory,
        publisher=RabbitEventPublisher(broker, topology),
        batch_size=10,
        retry_base_seconds=1,
        retry_max_seconds=60,
    )
    assert await relay.run_once() == 1

    async def notified() -> bool:
        payment = await api.queries.get(payment_id)
        return payment is not None and payment.webhook_delivered_at is not None

    await eventually(notified, message="webhook delivered")
    payment = await api.queries.get(payment_id)
    assert payment is not None
    assert payment.status is PaymentStatus.SUCCEEDED
    assert gateway.calls == [payment_id]
    assert [hook.payment_id for _, hook in webhooks.delivered] == [payment_id]


async def test_failing_webhook_is_retried_with_growing_delays_then_dead_lettered(
    api: ApiContainer, broker: RabbitBroker, topology: Topology, start_consumer: StartConsumer
) -> None:
    gateway, webhooks = FakeGateway(), FakeWebhooks(failures=100)
    await start_consumer(gateway, webhooks)
    payment_id = await new_payment(api)
    event_id = uuid4()
    await publish_created(broker, topology, payment_id, event_id)

    dlq: list[object] = []

    async def dead_lettered() -> bool:
        dlq.extend(await dead_letters(broker, topology))
        return bool(dlq)

    await eventually(dead_lettered, message="message in the DLQ")

    assert webhooks.attempts == 3
    first_gap = webhooks.attempted_at[1] - webhooks.attempted_at[0]
    second_gap = webhooks.attempted_at[2] - webhooks.attempted_at[1]
    assert 0.1 <= first_gap < second_gap  # 0.1 s, then 0.2 s
    assert second_gap >= 0.2
    assert gateway.calls == [payment_id]  # charged once, only the webhook was retried

    (message,) = dlq
    assert message.message_id == str(event_id)  # type: ignore[attr-defined]
    headers = message.headers  # type: ignore[attr-defined]
    assert headers[ATTEMPT_HEADER] == 3
    assert "HTTP 500" in str(headers[LAST_ERROR_HEADER])
    assert headers["x-death"][0]["reason"] == "rejected"
    assert json.loads(message.body)["payment_id"] == str(payment_id)  # type: ignore[attr-defined]

    payment = await api.queries.get(payment_id)
    assert payment is not None
    assert payment.status is PaymentStatus.SUCCEEDED  # the result itself is kept
    assert payment.webhook_delivered_at is None


async def test_replayed_dead_letter_is_processed(
    api: ApiContainer, broker: RabbitBroker, topology: Topology, start_consumer: StartConsumer
) -> None:
    """The client's server was down long enough for the DLQ; it is fixed, the message is
    replayed and only the webhook is sent: the payment is not charged again."""
    gateway, webhooks = FakeGateway(), FakeWebhooks(failures=3)
    await start_consumer(gateway, webhooks)
    payment_id = await new_payment(api)
    await publish_created(broker, topology, payment_id)

    async def in_dlq() -> bool:
        return await dead_letter_count(broker, topology) == 1

    await eventually(in_dlq, message="message in the DLQ")

    assert await replay_dead_letters(broker, topology) == 1

    async def delivered() -> bool:
        return bool(webhooks.delivered)

    await eventually(delivered, message="delivered after the replay")
    assert webhooks.attempts == 4
    assert gateway.calls == [payment_id]
    assert await dead_letter_count(broker, topology) == 0


async def test_webhook_that_recovers_is_delivered_without_the_dlq(
    api: ApiContainer, broker: RabbitBroker, topology: Topology, start_consumer: StartConsumer
) -> None:
    webhooks = FakeWebhooks(failures=2)
    await start_consumer(FakeGateway(), webhooks)
    payment_id = await new_payment(api)
    await publish_created(broker, topology, payment_id)

    async def delivered() -> bool:
        return bool(webhooks.delivered)

    await eventually(delivered, message="delivered on the third attempt")
    assert webhooks.attempts == 3
    assert await dead_letters(broker, topology) == []


async def test_retry_copy_that_cannot_be_published_goes_to_the_dlq(
    api: ApiContainer, broker: RabbitBroker, topology: Topology, start_consumer: StartConsumer
) -> None:
    """The delay queue is gone (deleted by hand, say): the copy is unroutable, publish()
    raises, and the original goes to the DLQ instead of spinning in payments.new."""
    first_delay = topology.retry_queue(topology.retry_delays_ms[0])
    await (await broker.declare_queue(first_delay)).delete(if_unused=False, if_empty=False)
    webhooks = FakeWebhooks(failures=100)
    await start_consumer(FakeGateway(), webhooks)
    await publish_created(broker, topology, await new_payment(api))

    async def in_dlq() -> bool:
        return await dead_letter_count(broker, topology) == 1

    await eventually(in_dlq, message="message in the DLQ")
    assert webhooks.attempts == 1


async def test_broken_message_goes_straight_to_the_dlq(
    broker: RabbitBroker, topology: Topology, start_consumer: StartConsumer
) -> None:
    gateway, webhooks = FakeGateway(), FakeWebhooks()
    await start_consumer(gateway, webhooks)
    await broker.publish(
        b"{not json",
        exchange=topology.exchange,
        routing_key=PAYMENT_CREATED,
        content_type="application/json",
    )

    dlq: list[object] = []

    async def dead_lettered() -> bool:
        dlq.extend(await dead_letters(broker, topology))
        return bool(dlq)

    await eventually(dead_lettered, message="broken message in the DLQ")
    assert ATTEMPT_HEADER not in dlq[0].headers  # type: ignore[attr-defined]  # no retries
    assert gateway.calls == []


async def test_event_for_an_unknown_payment_goes_straight_to_the_dlq(
    broker: RabbitBroker, topology: Topology, start_consumer: StartConsumer
) -> None:
    await start_consumer(FakeGateway(), FakeWebhooks())
    await publish_created(broker, topology, uuid4())

    async def dead_lettered() -> bool:
        return bool(await dead_letters(broker, topology))

    await eventually(dead_lettered, message="unknown payment in the DLQ")


async def test_redelivered_event_does_not_charge_again(
    api: ApiContainer, broker: RabbitBroker, topology: Topology, start_consumer: StartConsumer
) -> None:
    """The relay crashed after publishing and before its commit: the event goes out twice."""
    gateway, webhooks = FakeGateway(), FakeWebhooks()
    reports = (await start_consumer(gateway, webhooks)).reports
    payment_id = await new_payment(api)
    event_id = uuid4()

    await publish_created(broker, topology, payment_id, event_id)
    await eventually(lambda: _length_is(reports, 1), message="first delivery processed")
    await publish_created(broker, topology, payment_id, event_id)
    await eventually(lambda: _length_is(reports, 2), message="duplicate processed")

    first, duplicate = reports
    assert (first.charged, first.webhook_sent) == (True, True)
    assert (duplicate.charged, duplicate.webhook_sent) == (False, False)
    assert gateway.calls == [payment_id]
    assert len(webhooks.delivered) == 1


async def _length_is(items: list[ProcessingReport], n: int) -> bool:
    return len(items) == n
