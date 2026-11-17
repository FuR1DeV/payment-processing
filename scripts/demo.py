"""Runs the main scenarios against the live stack: docker compose exec api python -m scripts.demo

1. Ten payments: each ends as succeeded or failed and the client gets a webhook.
2. Idempotency: the same key returns the same payment, another body with it is rejected.
3. The client's server is down: three attempts with growing pauses, then the DLQ; the server
   is fixed, the dead letter is replayed and the webhook arrives.
4. The client's server is down for a while: the circuit opens, webhooks wait without
   requests, and all of them arrive once it is back.
"""

import asyncio
import os
import sys
import time
from typing import Any
from uuid import uuid4

import httpx
from faststream.rabbit import RabbitBroker

from app.core.config import get_settings
from app.messaging.broker import create_broker
from app.messaging.dlq import dead_letter_count, replay_dead_letters
from app.messaging.topology import Topology

API_URL = os.environ.get("DEMO_API_URL", "http://localhost:8000")
RECEIVER_URL = os.environ.get("DEMO_RECEIVER_URL", "http://webhook-receiver:9000")
WEBHOOK_URL = f"{RECEIVER_URL}/webhook"


def check(condition: bool, message: str) -> None:
    if not condition:
        print(f"FAIL: {message}")
        sys.exit(1)


async def create(api: httpx.AsyncClient, key: str | None = None, **body: Any) -> httpx.Response:
    payload = {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Loan repayment",
        "metadata": {"source": "demo"},
        "webhook_url": WEBHOOK_URL,
    } | body
    return await api.post(
        "/api/v1/payments", json=payload, headers={"Idempotency-Key": key or uuid4().hex}
    )


async def wait_for(
    api: httpx.AsyncClient, payment_id: str, field: str, within: float = 30
) -> dict[str, Any]:
    deadline = time.monotonic() + within
    while True:
        payment = (await api.get(f"/api/v1/payments/{payment_id}")).json()
        if payment[field] not in (None, "pending") or time.monotonic() > deadline:
            return payment
        await asyncio.sleep(0.5)


async def scenario_payments(api: httpx.AsyncClient) -> None:
    print("1. Ten payments, results arrive as webhooks")
    ids = [(await create(api, amount=f"{100 + i}.00")).json()["payment_id"] for i in range(10)]
    payments = await asyncio.gather(*(wait_for(api, pid, "webhook_delivered_at") for pid in ids))
    for p in payments:
        reason = f" ({p['failure_reason']})" if p["failure_reason"] else ""
        print(f"   {p['payment_id']}  {p['amount']:>8} {p['currency']}  {p['status']}{reason}")
        check(p["status"] in ("succeeded", "failed"), f"{p['payment_id']} not processed")
        check(p["webhook_delivered_at"] is not None, f"{p['payment_id']} webhook not delivered")
    succeeded = sum(p["status"] == "succeeded" for p in payments)
    print(f"   succeeded {succeeded}, failed {len(payments) - succeeded}, all clients notified")


async def scenario_idempotency(api: httpx.AsyncClient) -> None:
    print("2. Idempotency-Key")
    key = uuid4().hex
    first, again = await create(api, key=key), await create(api, key=key)
    check(first.json()["payment_id"] == again.json()["payment_id"], "replay created a new payment")
    check(again.headers.get("Idempotent-Replayed") == "true", "replay header missing")
    other = await create(api, key=key, amount="1.00")
    check(other.status_code == 422, f"another body with the same key gave {other.status_code}")
    print(f"   same key: same payment {first.json()['payment_id']}; another body: 422")
    # Let its webhook arrive before the next scenario breaks the receiver.
    await wait_for(api, first.json()["payment_id"], "webhook_delivered_at")


async def scenario_dead_letter(
    api: httpx.AsyncClient, receiver: httpx.AsyncClient, broker: RabbitBroker, topology: Topology
) -> None:
    print("3. The client's server answers 500: retries, then the DLQ")
    before = await dead_letter_count(broker, topology)
    await receiver.put("/mode", json={"fail": True})
    try:
        payment_id = (await create(api, amount="777.00")).json()["payment_id"]
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if await dead_letter_count(broker, topology) > before:
                break
            await asyncio.sleep(0.5)
    finally:
        await receiver.put("/mode", json={"fail": False})
    attempts = (await receiver.get("/received", params={"payment_id": payment_id})).json()
    for a in reversed(attempts):
        print(f"   {a['received_at']}  answered {a['answered']}")
    check(len(attempts) == 3, f"expected 3 delivery attempts, got {len(attempts)}")
    check(await dead_letter_count(broker, topology) == before + 1, "the message is not in the DLQ")
    payment = (await api.get(f"/api/v1/payments/{payment_id}")).json()
    check(payment["webhook_delivered_at"] is None, "webhook marked as delivered")
    print(f"   payment {payment['status']}, webhook not delivered, the message is in the DLQ")

    print("   the receiver works again: python -m app.messaging.dlq --replay")
    moved = await replay_dead_letters(broker, topology)
    payment = await wait_for(api, payment_id, "webhook_delivered_at")
    check(payment["webhook_delivered_at"] is not None, "webhook not delivered after the replay")
    print(f"   {moved} message(s) moved back, webhook delivered, nothing charged twice")


async def scenario_circuit_breaker(
    api: httpx.AsyncClient, receiver: httpx.AsyncClient, broker: RabbitBroker, topology: Topology
) -> None:
    print("4. The client's server is down for a while: the circuit breaker")
    before = await dead_letter_count(broker, topology)
    started = time.monotonic()
    await receiver.put("/mode", json={"fail": True})
    try:
        ids = [(await create(api, amount=f"{300 + i}.00")).json()["payment_id"] for i in range(8)]
        await asyncio.sleep(12)
        hits = [r for r in (await receiver.get("/received")).json() if r["payment_id"] in ids]
        print(f"   server down for 12 s: {len(hits)} requests for 8 payments, then the circuit")
        print("   opened and the rest wait without requests and without spending attempts")
    finally:
        await receiver.put("/mode", json={"fail": False})
    payments = await asyncio.gather(
        *(wait_for(api, pid, "webhook_delivered_at", within=90) for pid in ids)
    )
    delivered = sum(p["webhook_delivered_at"] is not None for p in payments)
    hits = [r for r in (await receiver.get("/received")).json() if r["payment_id"] in ids]
    failed = sum(r["answered"] != 200 for r in hits)
    print(
        f"   server is back: {delivered}/8 delivered after {time.monotonic() - started:.0f} s,"
        f" {failed} failed requests in total (24 without the breaker), DLQ unchanged"
    )
    check(delivered == 8, "not every webhook was delivered")
    check(failed < 24, "the breaker did not save any request")
    check(await dead_letter_count(broker, topology) == before, "something went to the DLQ")


async def main() -> None:
    settings = get_settings()
    topology = Topology.from_settings(settings)
    broker = create_broker(settings)
    await broker.connect()
    headers = {"X-API-Key": settings.api_key.get_secret_value()}
    try:
        async with (
            httpx.AsyncClient(base_url=API_URL, headers=headers, timeout=10) as api,
            httpx.AsyncClient(base_url=RECEIVER_URL, timeout=10) as receiver,
        ):
            await scenario_payments(api)
            await scenario_idempotency(api)
            await scenario_dead_letter(api, receiver, broker, topology)
            await scenario_circuit_breaker(api, receiver, broker, topology)
    finally:
        await broker.stop()
    print("OK")


if __name__ == "__main__":
    asyncio.run(main())
