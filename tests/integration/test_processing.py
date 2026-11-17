"""The consumer's use case on a real PostgreSQL: charge once, settle once, notify until
delivered. Each test runs it the way redeliveries would."""

import asyncio
from collections.abc import AsyncIterator, Callable
from decimal import Decimal
from uuid import uuid4

import pytest

from app.bootstrap import ApiContainer, ProcessorContainer, build_processor
from app.core.config import Settings
from app.domain.payment import PaymentStatus
from app.services.ports import GatewayUnavailableError, WebhookDeliveryError
from app.services.processing import PaymentNotFoundError
from tests.fakes import APPROVED, FakeGateway, FakeWebhooks, declined

from .helpers import new_payment

type ProcessorFactory = Callable[[FakeGateway, FakeWebhooks], ProcessorContainer]


@pytest.fixture
async def processor(settings: Settings) -> AsyncIterator[ProcessorFactory]:
    built: list[ProcessorContainer] = []

    def make(gateway: FakeGateway, webhooks: FakeWebhooks) -> ProcessorContainer:
        built.append(build_processor(settings, gateway=gateway, webhooks=webhooks))
        return built[-1]

    yield make
    for container in built:
        await container.close()


async def test_approved_payment_succeeds_and_client_is_notified(
    api: ApiContainer, processor: ProcessorFactory
) -> None:
    gateway, webhooks = FakeGateway(APPROVED), FakeWebhooks()
    payment_id = await new_payment(api)

    report = await processor(gateway, webhooks).process_payment(payment_id)

    assert (report.status, report.charged, report.webhook_sent) == ("succeeded", True, True)
    payment = await api.queries.get(payment_id)
    assert payment is not None
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    url, hook = webhooks.delivered[0]
    assert url == "http://client.test/webhook"
    assert hook.event_type == "payment.succeeded"
    assert (hook.payment_id, hook.amount, hook.metadata) == (
        payment_id,
        Decimal("250.50"),
        {"order_id": "A-7"},
    )


async def test_declined_payment_fails_and_client_is_notified(
    api: ApiContainer, processor: ProcessorFactory
) -> None:
    gateway, webhooks = FakeGateway(declined("insufficient_funds")), FakeWebhooks()
    payment_id = await new_payment(api)

    report = await processor(gateway, webhooks).process_payment(payment_id)

    assert report.status is PaymentStatus.FAILED
    payment = await api.queries.get(payment_id)
    assert payment is not None
    assert (payment.status, payment.failure_reason) == ("failed", "insufficient_funds")
    assert webhooks.delivered[0][1].event_type == "payment.failed"
    assert webhooks.delivered[0][1].failure_reason == "insufficient_funds"


async def test_long_decline_reason_is_cut_to_fit(
    api: ApiContainer, processor: ProcessorFactory
) -> None:
    """A real gateway may send a long reason; the decision must still be stored."""
    payment_id = await new_payment(api)
    await processor(FakeGateway(declined("x" * 300)), FakeWebhooks()).process_payment(payment_id)
    payment = await api.queries.get(payment_id)
    assert payment is not None
    assert payment.status is PaymentStatus.FAILED
    assert payment.failure_reason == "x" * 64


async def test_redelivery_of_a_finished_payment_does_nothing(
    api: ApiContainer, processor: ProcessorFactory
) -> None:
    gateway, webhooks = FakeGateway(), FakeWebhooks()
    process = processor(gateway, webhooks).process_payment
    payment_id = await new_payment(api)
    await process(payment_id)

    again = await process(payment_id)

    assert (again.charged, again.webhook_sent) == (False, False)
    assert len(gateway.calls) == 1
    assert len(webhooks.delivered) == 1


async def test_webhook_failure_keeps_the_result_and_retry_only_notifies(
    api: ApiContainer, processor: ProcessorFactory
) -> None:
    gateway, webhooks = FakeGateway(APPROVED), FakeWebhooks(failures=1)
    process = processor(gateway, webhooks).process_payment
    payment_id = await new_payment(api)

    with pytest.raises(WebhookDeliveryError):
        await process(payment_id)
    payment = await api.queries.get(payment_id)
    assert payment is not None
    assert payment.status is PaymentStatus.SUCCEEDED  # charged and recorded
    assert payment.webhook_delivered_at is None

    retry = await process(payment_id)

    assert (retry.charged, retry.webhook_sent) == (False, True)
    assert len(gateway.calls) == 1  # the retry did not charge again
    assert webhooks.attempts == 2


async def test_gateway_outage_leaves_the_payment_pending_for_a_retry(
    api: ApiContainer, processor: ProcessorFactory
) -> None:
    gateway = FakeGateway(GatewayUnavailableError("timeout"), APPROVED)
    webhooks = FakeWebhooks()
    process = processor(gateway, webhooks).process_payment
    payment_id = await new_payment(api)

    with pytest.raises(GatewayUnavailableError):
        await process(payment_id)
    payment = await api.queries.get(payment_id)
    assert payment is not None
    assert payment.status is PaymentStatus.PENDING
    assert webhooks.attempts == 0

    report = await process(payment_id)
    assert (report.status, report.charged) == ("succeeded", True)


async def test_parallel_runs_settle_the_payment_once(
    api: ApiContainer, processor: ProcessorFactory
) -> None:
    """Two consumers got the same message (the relay published it twice). Both see pending
    and ask the gateway; even if the answers differed, the first UPDATE wins and both
    runs continue with the stored status."""
    gateway = FakeGateway(APPROVED, declined(), delay=0.05)
    webhooks = FakeWebhooks()
    container = processor(gateway, webhooks)
    payment_id = await new_payment(api)

    reports = await asyncio.gather(
        container.process_payment(payment_id), container.process_payment(payment_id)
    )

    payment = await api.queries.get(payment_id)
    assert payment is not None
    assert {r.status for r in reports} == {payment.status}
    assert {hook.status for _, hook in webhooks.delivered} == {payment.status}
    assert len(gateway.calls) == 2
    # At-least-once: both runs may notify; the client deduplicates by payment_id.
    assert 1 <= len(webhooks.delivered) <= 2


async def test_unknown_payment_is_reported(processor: ProcessorFactory) -> None:
    with pytest.raises(PaymentNotFoundError):
        await processor(FakeGateway(), FakeWebhooks()).process_payment(uuid4())
