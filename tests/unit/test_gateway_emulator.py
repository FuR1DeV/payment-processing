from decimal import Decimal
from uuid import uuid4

from app.domain.payment import Currency
from app.integrations.gateway import DECLINE_REASONS, EmulatedGateway


class RecordingSleep:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


def gateway(sleep: RecordingSleep, success_rate: float = 0.9) -> EmulatedGateway:
    return EmulatedGateway(min_delay=2, max_delay=5, success_rate=success_rate, sleep=sleep)


async def test_takes_two_to_five_seconds() -> None:
    sleep = RecordingSleep()
    gw = gateway(sleep)
    for _ in range(50):
        await gw.charge(uuid4(), Decimal(1), Currency.RUB)
    assert all(2 <= s <= 5 for s in sleep.calls)


async def test_about_ninety_percent_are_approved() -> None:
    gw = gateway(RecordingSleep())
    results = [await gw.charge(uuid4(), Decimal(1), Currency.RUB) for _ in range(2000)]
    approved = sum(r.approved for r in results)
    assert 1700 < approved < 1900  # 90% +- 5 points: random ids, not a flaky threshold
    declined = [r for r in results if not r.approved]
    assert all(r.decline_reason in DECLINE_REASONS for r in declined)


async def test_same_payment_gets_the_same_answer() -> None:
    gw = gateway(RecordingSleep(), success_rate=0.5)
    for _ in range(20):
        payment_id = uuid4()
        first = await gw.charge(payment_id, Decimal(1), Currency.RUB)
        again = await gw.charge(payment_id, Decimal(1), Currency.RUB)
        assert first == again
