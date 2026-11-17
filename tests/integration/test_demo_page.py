"""The demo page: static files, helper endpoints behind the API key, switched off by a flag."""

from collections.abc import AsyncIterator

import httpx
import pytest
from faststream.rabbit import RabbitBroker

from app.bootstrap import ApiContainer, build_api
from app.core.config import Settings
from app.main import create_app
from app.messaging.topology import Topology
from webhook_receiver.app import create_app as create_receiver

from .helpers import API_KEY, WEBHOOK_SECRET, create_payment, make_settings

KEY = {"X-API-Key": API_KEY}


async def test_page_is_served_and_uses_only_text(client: httpx.AsyncClient) -> None:
    page = await client.get("/demo", follow_redirects=True)
    assert page.status_code == 200
    assert page.headers["content-type"].startswith("text/html")
    assert 'src="demo.js"' in page.text

    script = await client.get("/demo/demo.js")
    assert script.status_code == 200
    for endpoint in (
        '"/api/v1/payments"',
        '"/demo/api/queues"',
        '"/demo/api/receiver/mode"',
        '"/demo/api/dlq/replay"',
    ):
        assert endpoint in script.text
    assert "innerHTML" not in script.text  # API data goes into the page as text only
    assert "(скрыт)" in script.text  # the "API requests" block masks X-API-Key


async def test_helpers_need_the_api_key(client: httpx.AsyncClient) -> None:
    for path in ("/demo/api/queues", "/demo/api/receiver/mode"):
        assert (await client.get(path)).status_code == 401


async def test_outbox_of_a_payment(client: httpx.AsyncClient) -> None:
    payment_id = (await create_payment(client)).json()["payment_id"]
    response = await client.get(f"/demo/api/payments/{payment_id}/outbox", headers=KEY)
    assert response.status_code == 200
    (event,) = response.json()
    assert event["event_type"] == "payment.created"
    assert event["published_at"] is None
    assert event["attempts"] == 0


@pytest.fixture
async def with_receiver(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    """The API with its demo helpers talking to the webhook receiver in process."""
    receiver = httpx.ASGITransport(app=create_receiver(WEBHOOK_SECRET))
    container: ApiContainer = build_api(settings, receiver_transport=receiver)
    transport = httpx.ASGITransport(app=create_app(container))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    await container.close()


async def test_receiver_mode_is_switched_through_the_api(
    with_receiver: httpx.AsyncClient,
) -> None:
    assert (await with_receiver.get("/demo/api/receiver/mode", headers=KEY)).json() == {
        "fail": False
    }
    switched = await with_receiver.put("/demo/api/receiver/mode", json={"fail": True}, headers=KEY)
    assert switched.json() == {"fail": True}
    received = await with_receiver.get("/demo/api/receiver/received", headers=KEY)
    assert received.status_code == 200
    assert received.json() == []


async def test_unreachable_receiver_is_a_502() -> None:
    container = build_api(make_settings(demo_receiver_url="http://127.0.0.1:9"))  # nobody there
    try:
        transport = httpx.ASGITransport(app=create_app(container))
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/demo/api/receiver/mode", headers=KEY)
    finally:
        await container.close()
    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "unavailable"


async def test_queues_and_dlq_replay(
    client: httpx.AsyncClient, broker: RabbitBroker, topology: Topology
) -> None:
    await broker.publish(
        b"{}", exchange=topology.dead_letter_exchange, routing_key=topology.new_payments_queue_name
    )

    queues = {q["role"]: q for q in (await client.get("/demo/api/queues", headers=KEY)).json()}
    assert queues["dlq"]["messages"] == 1
    assert queues["main"]["name"] == topology.new_payments_queue_name
    assert {"retry", "postpone"} <= set(queues)

    replay = await client.post("/demo/api/dlq/replay", headers=KEY)
    assert replay.json() == {"moved": 1}
    queues = {q["role"]: q for q in (await client.get("/demo/api/queues", headers=KEY)).json()}
    assert queues["dlq"]["messages"] == 0
    assert queues["main"]["messages"] == 1


async def test_everything_is_gone_when_switched_off() -> None:
    container = build_api(make_settings(demo_page_enabled=False))
    try:
        app = create_app(container)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/demo/")).status_code == 404
            assert (await client.get("/demo/api/queues", headers=KEY)).status_code == 404
            assert (await client.get("/health")).status_code == 200
    finally:
        await container.close()
