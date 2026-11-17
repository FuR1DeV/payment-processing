"""Circuit breaker for outgoing webhooks, one circuit per client host.

If a client's server is down, there is no point in sending it every webhook and waiting
for the timeout each time: that burns our workers and the attempt budget of every message,
and hammers a server that is trying to come back. After a few failures in a row the
circuit opens, and deliveries to that host are postponed without a request.

    closed     requests go through; `failure_threshold` server-down failures in a row open it
    open       requests are refused for `open_seconds`
    half-open  then one request (a probe) goes through while the others are still refused;
               the probe's success closes the circuit, its failure opens it again

Every call gets a Permit that says whether it is the probe of a particular open period, so
a late answer to an older request can neither free nor fail someone else's probe.

The state lives in the memory of one consumer process. Several consumers keep separate
circuits, which is fine for this purpose: each of them stops hammering the host on its own.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlsplit

from app.domain.events import PaymentWebhook
from app.services.ports import WebhookDeliveryError, WebhookPostponedError, WebhookSender

# Hosts come from client input; past this many circuits the oldest one is forgotten
# (which only means "closed" for that host).
MAX_CIRCUITS = 10_000


@dataclass(eq=False)
class _Circuit:
    failures: int = 0
    open_until: float | None = None  # monotonic time; None: closed
    probe_in_flight: bool = False


@dataclass(frozen=True)
class Permit:
    key: str
    probe_of: _Circuit | None = None  # the open period this call probes, if it is the probe


class CircuitOpenError(Exception):
    def __init__(self, retry_after: float) -> None:
        super().__init__(f"circuit open, retry in {retry_after:.1f} s")
        self.retry_after = retry_after


class CircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int,
        open_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._threshold = failure_threshold
        self._open_seconds = open_seconds
        self._clock = clock
        self._circuits: dict[str, _Circuit] = {}

    def acquire(self, key: str) -> Permit:
        """A Permit to make the call, or CircuitOpenError.

        Synchronous on purpose: with asyncio nothing runs between the check and the update,
        so two concurrent calls cannot both become the half-open probe.
        """
        circuit = self._circuits.get(key)
        if circuit is None or circuit.open_until is None:
            return Permit(key)
        now = self._clock()
        if now < circuit.open_until:
            raise CircuitOpenError(circuit.open_until - now)
        if circuit.probe_in_flight:  # half-open, and someone is already probing
            raise CircuitOpenError(self._open_seconds)
        circuit.probe_in_flight = True
        return Permit(key, probe_of=circuit)

    def record_success(self, permit: Permit) -> None:
        # Any answer from the server closes the circuit, the probe's or a late one.
        self._circuits.pop(permit.key, None)

    def record_failure(self, permit: Permit) -> None:
        circuit = self._circuits.get(permit.key)
        if permit.probe_of is not None:
            if circuit is permit.probe_of:  # not closed by another answer meanwhile
                circuit.probe_in_flight = False
                circuit.open_until = self._clock() + self._open_seconds
            return
        if circuit is None:
            circuit = self._new_circuit(permit.key)
        if circuit.open_until is not None:
            return  # already open: a late failure of an older request adds nothing
        circuit.failures += 1
        if circuit.failures >= self._threshold:
            circuit.open_until = self._clock() + self._open_seconds
            circuit.failures = 0

    def release(self, permit: Permit) -> None:
        """The call ended without an answer (cancelled): if it was the probe, free the slot."""
        circuit = self._circuits.get(permit.key)
        if permit.probe_of is not None and circuit is permit.probe_of:
            circuit.probe_in_flight = False

    def state(self, key: str) -> str:
        circuit = self._circuits.get(key)
        if circuit is None or circuit.open_until is None:
            return "closed"
        return "open" if circuit.open_until > self._clock() else "half_open"

    def _new_circuit(self, key: str) -> _Circuit:
        if len(self._circuits) >= MAX_CIRCUITS:
            del self._circuits[next(iter(self._circuits))]  # the oldest one
        circuit = self._circuits[key] = _Circuit()
        return circuit


def circuit_key(url: str) -> str:
    """Host and port only: no path, and no user:password@ that could end up in the logs."""
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    return f"{host}:{parts.port}" if parts.port else host


class BreakerWebhookSender:
    """Wraps a WebhookSender with a circuit per host of the webhook URL."""

    def __init__(self, inner: WebhookSender, breaker: CircuitBreaker) -> None:
        self._inner = inner
        self._breaker = breaker

    async def send(self, url: str, webhook: PaymentWebhook) -> None:
        key = circuit_key(url)
        try:
            permit = self._breaker.acquire(key)
        except CircuitOpenError as exc:
            raise WebhookPostponedError(key, exc.retry_after) from exc
        try:
            await self._inner.send(url, webhook)
        except WebhookDeliveryError as exc:
            if exc.server_down:
                self._breaker.record_failure(permit)
            else:  # it answered 4xx: up, just unhappy with us
                self._breaker.record_success(permit)
            raise
        except BaseException:
            # E.g. cancelled on shutdown: a probe that never finishes must not keep the
            # circuit half-open for ever.
            self._breaker.release(permit)
            raise
        self._breaker.record_success(permit)
