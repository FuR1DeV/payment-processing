import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr

from app.domain.events import PaymentWebhook
from app.domain.payment import Currency, PaymentStatus
from app.integrations.webhooks import (
    ID_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    HttpWebhookSender,
    sign,
    verify,
)
from app.services.ports import WebhookDeliveryError

SECRET = b"test-webhook-secret-0123"
NOW = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)


def webhook() -> PaymentWebhook:
    return PaymentWebhook(
        event_type="payment.succeeded",
        payment_id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("1500.00"),
        currency=Currency.RUB,
        description="Loan repayment",
        metadata={"order_id": "A-1"},
        failure_reason=None,
        created_at=NOW,
        processed_at=NOW,
    )


def test_signature_round_trip() -> None:
    ts = str(int(NOW.timestamp()))
    signature = sign(SECRET, ts, b'{"a":1}')
    assert signature.startswith("sha256=")
    assert verify(SECRET, ts, b'{"a":1}', signature, now=NOW)


def test_tampered_body_wrong_secret_and_garbage_fail() -> None:
    ts = str(int(NOW.timestamp()))
    signature = sign(SECRET, ts, b'{"a":1}')
    assert not verify(SECRET, ts, b'{"a":2}', signature, now=NOW)
    assert not verify(b"another-secret-0123456", ts, b'{"a":1}', signature, now=NOW)
    assert not verify(SECRET, "not-a-number", b'{"a":1}', signature, now=NOW)
    assert not verify(SECRET, ts, b'{"a":1}', "", now=NOW)


def test_non_ascii_signature_is_false_not_an_error() -> None:
    ts = str(int(NOW.timestamp()))
    assert not verify(SECRET, ts, b"{}", "sha256=éé", now=NOW)


def test_old_signature_is_rejected() -> None:
    ts = str(int((NOW - timedelta(minutes=10)).timestamp()))
    assert not verify(SECRET, ts, b"{}", sign(SECRET, ts, b"{}"), now=NOW)


async def test_sender_signs_the_exact_body_it_sends() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = HttpWebhookSender(client, SecretStr(SECRET.decode()), clock=lambda: NOW)
        hook = webhook()
        await sender.send("http://client.test/webhook", hook)

    request = seen[0]
    assert request.headers[ID_HEADER] == str(hook.payment_id)
    assert verify(
        SECRET,
        request.headers[TIMESTAMP_HEADER],
        request.content,
        request.headers[SIGNATURE_HEADER],
        now=NOW,
    )
    payload = json.loads(request.content)
    assert payload["amount"] == "1500.00"  # a string: no float rounding on the client side
    assert payload["status"] == "succeeded"


@pytest.mark.parametrize(
    ("status_code", "server_down"),
    [(301, False), (400, False), (404, False), (429, True), (500, True), (503, True)],
)
async def test_non_2xx_is_a_delivery_error(status_code: int, server_down: bool) -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(status_code))
    async with httpx.AsyncClient(transport=transport) as client:
        sender = HttpWebhookSender(client, SecretStr(SECRET.decode()))
        with pytest.raises(WebhookDeliveryError, match=str(status_code)) as error:
            await sender.send("http://client.test/webhook", webhook())
    # Only "the server cannot take it" trips the circuit breaker; a 4xx comes from a live one.
    assert error.value.server_down is server_down


async def test_slow_server_hits_the_overall_deadline() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(1)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = HttpWebhookSender(client, SecretStr(SECRET.decode()), timeout_seconds=0.05)
        with pytest.raises(WebhookDeliveryError, match="TimeoutError"):
            await sender.send("http://client.test/webhook", webhook())


async def test_network_error_is_a_delivery_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = HttpWebhookSender(client, SecretStr(SECRET.decode()))
        with pytest.raises(WebhookDeliveryError, match="ConnectError") as error:
            await sender.send("http://client.test/webhook", webhook())
    assert error.value.server_down
