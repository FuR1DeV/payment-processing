from decimal import Decimal
from enum import StrEnum


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def is_final(self) -> bool:
        return self is not PaymentStatus.PENDING


# RUB, USD and EUR all have two minor units (kopecks, cents).
AMOUNT_SCALE = Decimal("0.01")


def normalize_amount(amount: Decimal) -> Decimal:
    """100, 100.0 and 100.00 are the same amount: one representation for storage and hashing."""
    return amount.quantize(AMOUNT_SCALE)
