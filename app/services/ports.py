"""What the services need from the outside world. Implementations live in app/integrations
and app/messaging; tests substitute fakes."""

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from app.db.models import OutboxEvent
from app.db.repositories import UnitOfWork
from app.domain.events import PaymentWebhook
from app.domain.payment import Currency

type UnitOfWorkFactory = Callable[[], UnitOfWork]


@dataclass(frozen=True)
class ChargeResult:
    approved: bool
    decline_reason: str | None = None


class GatewayUnavailableError(Exception):
    """The gateway did not give an answer (timeout, 5xx). The outcome is unknown, so the
    charge is repeated later with the same idempotency key, never treated as a decline."""


class PaymentGateway(Protocol):
    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> ChargeResult:
        """Charge once per payment_id: a repeated call returns the first answer."""
        ...


class WebhookDeliveryError(Exception):
    """The client's endpoint did not accept the webhook (network error, timeout, non-2xx).

    server_down: the failure says the server is unavailable (no connection, timeout, 5xx,
    429), as opposed to a server that is up and answered 4xx. Only the former trips the
    circuit breaker.
    """

    def __init__(self, message: str, *, server_down: bool = True) -> None:
        super().__init__(message)
        self.server_down = server_down


class WebhookPostponedError(Exception):
    """Delivery was not even tried: the client's server is known to be down (open circuit).
    The caller should come back after retry_after seconds without counting an attempt."""

    def __init__(self, host: str, retry_after: float) -> None:
        super().__init__(f"circuit open for {host}, retry in {retry_after:.1f} s")
        self.host = host
        self.retry_after = retry_after


class WebhookSender(Protocol):
    async def send(self, url: str, webhook: PaymentWebhook) -> None:
        """Raise WebhookDeliveryError or WebhookPostponedError if it was not delivered."""
        ...


class EventPublisher(Protocol):
    async def publish(self, event: OutboxEvent) -> None:
        """Return only once the broker has confirmed the message; raise otherwise."""
        ...
