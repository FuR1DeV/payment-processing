import asyncio
from decimal import Decimal
from uuid import UUID

from app.db.models import OutboxEvent
from app.domain.events import PaymentWebhook
from app.domain.payment import Currency
from app.services.ports import ChargeResult, WebhookDeliveryError

APPROVED = ChargeResult(approved=True)


def declined(reason: str = "insufficient_funds") -> ChargeResult:
    return ChargeResult(approved=False, decline_reason=reason)


class FakeGateway:
    """Answers from a script (results or exceptions to raise), then approves."""

    def __init__(self, *script: ChargeResult | Exception, delay: float = 0) -> None:
        self.script = list(script)
        self.delay = delay
        self.calls: list[UUID] = []

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> ChargeResult:
        self.calls.append(payment_id)
        if self.delay:
            await asyncio.sleep(self.delay)
        item = self.script.pop(0) if self.script else APPROVED
        if isinstance(item, Exception):
            raise item
        return item


class FakeWebhooks:
    """The first `failures` deliveries fail with HTTP 500, the rest succeed."""

    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.attempts = 0
        self.attempted_at: list[float] = []  # loop time of every attempt
        self.delivered: list[tuple[str, PaymentWebhook]] = []

    async def send(self, url: str, webhook: PaymentWebhook) -> None:
        self.attempts += 1
        self.attempted_at.append(asyncio.get_running_loop().time())
        if self.failures > 0:
            self.failures -= 1
            raise WebhookDeliveryError("HTTP 500")
        self.delivered.append((url, webhook))


class FakePublisher:
    def __init__(
        self, *, fail: bool = False, delay: float = 0, unroutable: frozenset[str] = frozenset()
    ) -> None:
        self.fail = fail
        self.delay = delay
        self.unroutable = unroutable
        self.published: list[UUID] = []

    async def publish(self, event: OutboxEvent) -> None:
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail:
            raise ConnectionError("broker is down")
        if event.event_type in self.unroutable:
            raise LookupError(f"no queue bound for {event.event_type}")
        self.published.append(event.event_id)
