"""The circuit breaker for client webhooks, on real RabbitMQ and PostgreSQL. A lower
threshold and a 1 s open time keep the tests short."""

import asyncio

import pytest
from faststream.rabbit import RabbitBroker

from app.bootstrap import ApiContainer
from app.core.config import Settings
from app.messaging.dlq import dead_letter_count
from app.messaging.retry import ATTEMPT_HEADER, POSTPONED_HEADER
from app.messaging.topology import Topology
from tests.fakes import FakeGateway, FakeWebhooks

from .conftest import RunningConsumer, StartConsumer
from .helpers import eventually, make_settings, new_payment, publish_created


@pytest.fixture
def settings() -> Settings:
    return make_settings(
        webhook_breaker_failures=2,
        webhook_breaker_open_seconds=1.0,
        webhook_max_postponements=1,
    )


def open_circuit(consumer: RunningConsumer, host: str = "client.test") -> None:
    breaker = consumer.processor.breaker
    for _ in range(2):
        breaker.record_failure(breaker.acquire(host))


async def test_circuit_opens_and_the_last_attempt_waits_instead_of_failing(
    api: ApiContainer, broker: RabbitBroker, topology: Topology, start_consumer: StartConsumer
) -> None:
    """The client's server is down. Two failed attempts open the circuit, so the third one
    is not made: the message waits in the postponement queue with its attempt counter
    untouched, instead of failing once more and going to the DLQ."""
    webhooks = FakeWebhooks(failures=100)
    await start_consumer(FakeGateway(), webhooks)
    await publish_created(broker, topology, await new_payment(api))
    postpone_queue = topology.retry_queue(topology.postpone_delay_ms)

    async def postponed() -> bool:
        queue = await broker.declare_queue(postpone_queue)
        return (await queue.declare()).message_count == 1

    await eventually(postponed, message="message in the postponement queue")
    assert webhooks.attempts == 2
    assert await dead_letter_count(broker, topology) == 0
    message = await (await broker.declare_queue(postpone_queue)).get(no_ack=True)
    assert message.headers[ATTEMPT_HEADER] == 3
    assert message.headers[POSTPONED_HEADER] == 1


async def test_open_circuit_delivers_after_the_probe_without_spending_attempts(
    api: ApiContainer, broker: RabbitBroker, topology: Topology, start_consumer: StartConsumer
) -> None:
    """The circuit for client.test is already open. The payment is charged, the webhook
    waits for the open period without a request, then goes out as the probe."""
    gateway, webhooks = FakeGateway(), FakeWebhooks()
    consumer = await start_consumer(gateway, webhooks)
    open_circuit(consumer)
    payment_id = await new_payment(api)
    started = asyncio.get_running_loop().time()

    await publish_created(broker, topology, payment_id)

    async def delivered() -> bool:
        return bool(webhooks.delivered)

    await eventually(delivered, message="delivered by the probe")
    assert asyncio.get_running_loop().time() - started >= 0.9  # waited for the open period
    assert webhooks.attempts == 1  # no request while the circuit was open
    assert gateway.calls == [payment_id]  # charged once, on the first delivery
    assert consumer.processor.breaker.state("client.test") == "closed"
    assert await dead_letter_count(broker, topology) == 0


async def test_postponed_too_many_times_goes_to_the_dlq(
    api: ApiContainer, broker: RabbitBroker, topology: Topology, start_consumer: StartConsumer
) -> None:
    """The host stays down. The message waits once (the limit here is 1), its probe fails,
    the retry finds the circuit open again: no more waiting, it goes to the DLQ. Without the
    limit it would circle through the delay queue for as long as the host is down."""
    webhooks = FakeWebhooks(failures=100)
    consumer = await start_consumer(FakeGateway(), webhooks)
    open_circuit(consumer)
    await publish_created(broker, topology, await new_payment(api))

    async def in_dlq() -> bool:
        return await dead_letter_count(broker, topology) == 1

    await eventually(in_dlq, message="message in the DLQ")
    assert webhooks.attempts == 1  # only the probe reached the server
