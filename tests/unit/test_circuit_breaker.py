import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from app.domain.events import PaymentWebhook
from app.domain.payment import Currency, PaymentStatus
from app.integrations import circuit_breaker as cb_module
from app.integrations.circuit_breaker import (
    BreakerWebhookSender,
    CircuitBreaker,
    CircuitOpenError,
    circuit_key,
)
from app.services.ports import WebhookDeliveryError, WebhookPostponedError


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def breaker(clock: Clock, threshold: int = 3, open_seconds: float = 30) -> CircuitBreaker:
    return CircuitBreaker(failure_threshold=threshold, open_seconds=open_seconds, clock=clock)


def fail(cb: CircuitBreaker, key: str = "a", times: int = 1) -> None:
    for _ in range(times):
        cb.record_failure(cb.acquire(key))


def refused(cb: CircuitBreaker, key: str = "a") -> float:
    with pytest.raises(CircuitOpenError) as error:
        cb.acquire(key)
    return error.value.retry_after


def test_opens_after_the_threshold_of_failures_in_a_row() -> None:
    cb = breaker(Clock())
    fail(cb, times=2)
    assert cb.state("a") == "closed"
    fail(cb)
    assert cb.state("a") == "open"
    assert refused(cb) == 30


def test_success_resets_the_count() -> None:
    cb = breaker(Clock())
    fail(cb, times=2)
    cb.record_success(cb.acquire("a"))
    fail(cb, times=2)
    assert cb.state("a") == "closed"


def test_half_open_lets_exactly_one_probe_through() -> None:
    clock = Clock()
    cb = breaker(clock, threshold=1)
    fail(cb)
    clock.now += 10
    assert refused(cb) == 20  # still open: comes back in 20 s
    clock.now += 20
    assert cb.state("a") == "half_open"
    probe = cb.acquire("a")
    assert probe.probe_of is not None
    assert refused(cb) == 30  # everyone else waits for the probe


def test_probe_failure_reopens_and_probe_success_closes() -> None:
    clock = Clock()
    cb = breaker(clock, threshold=3)
    fail(cb, times=3)
    clock.now += 30
    cb.record_failure(cb.acquire("a"))  # one failed probe is enough, whatever the threshold
    assert refused(cb) == 30
    clock.now += 30
    cb.record_success(cb.acquire("a"))
    assert cb.state("a") == "closed"


def test_late_failure_of_an_older_request_does_not_touch_the_probe() -> None:
    """A request sent before the circuit opened fails while the probe is in flight: it
    must neither free the probe slot (a second probe would go out) nor reopen the circuit."""
    clock = Clock()
    cb = breaker(clock, threshold=1, open_seconds=1)
    old = cb.acquire("a")  # in flight, slow
    fail(cb)  # opens
    clock.now += 1
    probe = cb.acquire("a")
    cb.record_failure(old)  # the old one finally times out
    assert refused(cb) == 1  # still exactly one probe
    cb.record_success(probe)
    assert cb.state("a") == "closed"


def test_cancelled_ordinary_call_does_not_free_the_probe() -> None:
    clock = Clock()
    cb = breaker(clock, threshold=1)
    other = cb.acquire("a")
    fail(cb)
    clock.now += 30
    cb.acquire("a")  # the probe
    cb.release(other)
    assert refused(cb) == 30


def test_late_failure_does_not_extend_the_open_period() -> None:
    clock = Clock()
    cb = breaker(clock, threshold=1)
    old = cb.acquire("a")
    fail(cb)
    clock.now += 20
    cb.record_failure(old)
    assert refused(cb) == 10


def test_probe_answer_after_the_circuit_was_closed_is_ignored() -> None:
    clock = Clock()
    cb = breaker(clock, threshold=1)
    old = cb.acquire("a")
    fail(cb)
    clock.now += 30
    probe = cb.acquire("a")
    cb.record_success(old)  # the server answered an older request: closed
    cb.record_failure(probe)  # stale news about a period that is over
    assert cb.state("a") == "closed"


def test_circuits_are_per_host_and_their_number_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cb_module, "MAX_CIRCUITS", 2)
    cb = breaker(Clock(), threshold=1)
    fail(cb, "a")
    assert cb.acquire("b") is not None  # other hosts are not affected
    fail(cb, "b")
    fail(cb, "c")  # "a" is the oldest and is forgotten
    assert cb.state("a") == "closed"
    assert cb.state("b") == cb.state("c") == "open"


@pytest.mark.parametrize(
    ("url", "key"),
    [
        ("https://Client.Test/hook", "client.test"),
        ("https://client.test:8443/a?b=c", "client.test:8443"),
        ("https://user:secret@client.test/hook", "client.test"),  # no credentials in logs
    ],
)
def test_circuit_key(url: str, key: str) -> None:
    assert circuit_key(url) == key


def webhook() -> PaymentWebhook:
    now = datetime.now(UTC)
    return PaymentWebhook(
        event_type="payment.succeeded",
        payment_id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("1.00"),
        currency=Currency.RUB,
        description=None,
        metadata={},
        failure_reason=None,
        created_at=now,
        processed_at=now,
    )


class Inner:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.calls: list[str] = []

    async def send(self, url: str, webhook: PaymentWebhook) -> None:
        self.calls.append(url)
        if self.error is not None:
            raise self.error


async def test_open_circuit_postpones_without_a_request() -> None:
    inner = Inner(WebhookDeliveryError("HTTP 503"))
    sender = BreakerWebhookSender(inner, breaker(Clock(), threshold=2))
    for _ in range(2):
        with pytest.raises(WebhookDeliveryError):
            await sender.send("https://client.test/hook", webhook())
    with pytest.raises(WebhookPostponedError) as postponed:
        await sender.send("https://client.test/other", webhook())
    assert len(inner.calls) == 2
    assert postponed.value.host == "client.test"
    assert postponed.value.retry_after == 30


async def test_4xx_means_the_server_is_up_and_never_opens_the_circuit() -> None:
    inner = Inner(WebhookDeliveryError("HTTP 404", server_down=False))
    sender = BreakerWebhookSender(inner, breaker(Clock(), threshold=2))
    for _ in range(5):
        with pytest.raises(WebhookDeliveryError):
            await sender.send("https://client.test/hook", webhook())
    assert len(inner.calls) == 5


async def test_host_and_port_make_the_circuit() -> None:
    inner = Inner(WebhookDeliveryError("HTTP 500"))
    sender = BreakerWebhookSender(inner, breaker(Clock(), threshold=1))
    with pytest.raises(WebhookDeliveryError):
        await sender.send("https://Client.Test:8443/a", webhook())
    with pytest.raises(WebhookPostponedError):
        await sender.send("https://client.test:8443/b", webhook())  # same host and port
    with pytest.raises(WebhookDeliveryError):
        await sender.send("https://client.test/c", webhook())  # another port: another circuit


async def test_cancelled_probe_does_not_leave_the_circuit_stuck() -> None:
    clock = Clock()
    cb = breaker(clock, threshold=1)
    fail(cb, "client.test")
    clock.now += 30
    sender = BreakerWebhookSender(Inner(asyncio.CancelledError()), cb)
    with pytest.raises(asyncio.CancelledError):
        await sender.send("https://client.test/hook", webhook())
    assert cb.acquire("client.test").probe_of is not None  # the next call may probe
