import asyncio
import random
from collections.abc import Awaitable, Callable
from decimal import Decimal
from uuid import UUID

from app.domain.payment import Currency
from app.services.ports import ChargeResult

DECLINE_REASONS = ("insufficient_funds", "do_not_honor", "card_expired")


class EmulatedGateway:
    """Stand-in for an external payment gateway: answers in 2-5 s, approves 90% of payments.

    The outcome depends only on payment_id (it seeds the random generator), so a repeated
    call for the same payment gets the same answer. That is how a real gateway treats a
    repeated idempotency key, and it keeps a redelivered message from changing the result.
    """

    def __init__(
        self,
        *,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._sleep = sleep

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> ChargeResult:
        await self._sleep(random.uniform(self._min_delay, self._max_delay))
        outcome = random.Random(payment_id.int)
        if outcome.random() < self._success_rate:
            return ChargeResult(approved=True)
        return ChargeResult(approved=False, decline_reason=outcome.choice(DECLINE_REASONS))
