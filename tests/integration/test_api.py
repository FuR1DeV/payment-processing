"""HTTP API on a real PostgreSQL: creation, reading, idempotency, races, authentication."""

import asyncio
from collections import Counter
from uuid import uuid4

import httpx
import pytest

import app.services.payments as payments_service
from app.db.models import OutboxEvent

from .helpers import API_KEY, create_payment, one_row, payment_body, scalar


async def test_create_returns_202_and_writes_payment_with_its_event(
    client: httpx.AsyncClient,
) -> None:
    response = await create_payment(client)
    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == "pending"
    assert response.headers["Location"] == f"/api/v1/payments/{body['payment_id']}"
    assert "Idempotent-Replayed" not in response.headers

    assert await scalar("SELECT count(*) FROM payments") == 1
    event = await one_row("SELECT * FROM outbox")
    assert str(event["aggregate_id"]) == body["payment_id"]
    assert event["event_type"] == "payment.created"
    assert event["payload"]["payment_id"] == body["payment_id"]
    assert event["published_at"] is None


async def test_get_returns_details(client: httpx.AsyncClient) -> None:
    key = uuid4().hex
    created = (await create_payment(client, key=key, amount=1500)).json()
    response = await client.get(
        f"/api/v1/payments/{created['payment_id']}", headers={"X-API-Key": API_KEY}
    )
    assert response.status_code == 200
    assert response.json() == {
        "payment_id": created["payment_id"],
        "status": "pending",
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Loan repayment",
        "metadata": {"order_id": "A-1001", "client": {"id": 42}},
        "failure_reason": None,
        "idempotency_key": key,
        "webhook_url": "http://client.test/webhook",
        "created_at": created["created_at"],
        "processed_at": None,
        "webhook_delivered_at": None,
    }


async def test_unknown_payment_is_404(client: httpx.AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid4()}", headers={"X-API-Key": API_KEY})
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "not_found"


async def test_replay_returns_the_same_payment_and_creates_nothing(
    client: httpx.AsyncClient,
) -> None:
    key = uuid4().hex
    first = await create_payment(client, key=key)
    second = await create_payment(client, key=key)
    assert second.status_code == 202
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json() == first.json()
    assert await scalar("SELECT count(*) FROM payments") == 1
    assert await scalar("SELECT count(*) FROM outbox") == 1


async def test_replay_with_an_equivalent_body_is_still_a_replay(client: httpx.AsyncClient) -> None:
    key = uuid4().hex
    first = await create_payment(client, key=key, amount="1500.00", metadata={"a": 1, "b": 2})
    second = await create_payment(client, key=key, amount=1500, metadata={"b": 2, "a": 1})
    assert second.json()["payment_id"] == first.json()["payment_id"]


async def test_same_key_with_another_body_is_rejected(client: httpx.AsyncClient) -> None:
    key = uuid4().hex
    assert (await create_payment(client, key=key, amount="100.00")).status_code == 202
    reused = await create_payment(client, key=key, amount="999.00")
    assert reused.status_code == 422
    assert reused.json()["detail"]["code"] == "idempotency_key_reused"
    assert await scalar("SELECT count(*) FROM payments") == 1


async def test_concurrent_requests_with_one_key_create_one_payment(
    client: httpx.AsyncClient,
) -> None:
    key = uuid4().hex
    responses = await asyncio.gather(*(create_payment(client, key=key) for _ in range(10)))
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert Counter(r.headers.get("Idempotent-Replayed") for r in responses) == {
        None: 1,
        "true": 9,
    }
    assert await scalar("SELECT count(*) FROM payments") == 1
    assert await scalar("SELECT count(*) FROM outbox") == 1


async def test_concurrent_requests_with_one_key_and_different_bodies(
    client: httpx.AsyncClient,
) -> None:
    key = uuid4().hex
    requests = [create_payment(client, key=key, amount=str(100 + i % 2)) for i in range(10)]
    responses = await asyncio.gather(*requests)
    accepted = [r for r in responses if r.status_code == 202]
    assert {r.status_code for r in responses} <= {202, 422}
    assert len({r.json()["payment_id"] for r in accepted}) == 1
    assert len(accepted) == 5  # the five with the winner's body
    assert await scalar("SELECT count(*) FROM payments") == 1


@pytest.mark.parametrize("key_header", [{}, {"Idempotency-Key": ""}, {"Idempotency-Key": "  "}])
async def test_missing_idempotency_key_is_400(
    client: httpx.AsyncClient, key_header: dict[str, str]
) -> None:
    response = await client.post(
        "/api/v1/payments", json=payment_body(), headers={"X-API-Key": API_KEY} | key_header
    )
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "idempotency_key_required"


async def test_amount_as_a_fractional_json_number_is_rejected(client: httpx.AsyncClient) -> None:
    response = await create_payment(client, amount=999999999999999.99)
    assert response.status_code == 422
    assert await scalar("SELECT count(*) FROM payments") == 0


async def test_too_long_idempotency_key_is_rejected(client: httpx.AsyncClient) -> None:
    assert (await create_payment(client, key="k" * 256)).status_code == 422


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": ""}, {"X-API-Key": "wrong-key-0123456789"}])
async def test_every_endpoint_requires_the_api_key(
    client: httpx.AsyncClient, headers: dict[str, str]
) -> None:
    post = await client.post(
        "/api/v1/payments", json=payment_body(), headers=headers | {"Idempotency-Key": "k-1"}
    )
    get = await client.get(f"/api/v1/payments/{uuid4()}", headers=headers)
    for response in (post, get):
        assert response.status_code == 401
        assert response.json()["detail"]["code"] == "unauthorized"
        assert response.headers["WWW-Authenticate"] == "ApiKey"
    assert await scalar("SELECT count(*) FROM payments") == 0


async def test_health_needs_no_key(client: httpx.AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "postgres": "ok"}


async def test_payment_is_not_saved_when_its_event_cannot_be(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Outbox atomicity: the event row fails at the database level (NOT NULL), and the
    payment written a moment earlier in the same transaction is gone as well."""
    original = payments_service._payment_created

    def broken(*args: object) -> OutboxEvent:
        event = original(*args)  # type: ignore[arg-type]
        event.event_type = None  # type: ignore[assignment]
        return event

    monkeypatch.setattr(payments_service, "_payment_created", broken)
    with pytest.raises(Exception, match="event_type"):
        await create_payment(client)
    assert await scalar("SELECT count(*) FROM payments") == 0
    assert await scalar("SELECT count(*) FROM outbox") == 0
