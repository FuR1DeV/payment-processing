from decimal import Decimal
from typing import Any

from app.domain.payment import Currency, normalize_amount
from app.services.payments import CreatePaymentCommand


def command(**overrides: Any) -> CreatePaymentCommand:
    values: dict[str, Any] = {
        "idempotency_key": "key-1",
        "amount": Decimal("100.00"),
        "currency": Currency.RUB,
        "description": "Loan repayment",
        "webhook_url": "http://client.test/webhook",
        "metadata": {"order_id": "A-1", "nested": {"a": 1, "b": 2}},
    }
    values.update(overrides)
    return CreatePaymentCommand(**values)


def test_same_body_written_differently_has_the_same_fingerprint() -> None:
    a = command(amount=Decimal(100), metadata={"order_id": "A-1", "nested": {"a": 1, "b": 2}})
    b = command(amount=Decimal("100.0"), metadata={"nested": {"b": 2, "a": 1}, "order_id": "A-1"})
    assert a.fingerprint() == b.fingerprint()


def test_any_real_difference_changes_the_fingerprint() -> None:
    base = command().fingerprint()
    assert command(amount=Decimal("100.01")).fingerprint() != base
    assert command(currency=Currency.USD).fingerprint() != base
    assert command(description=None).fingerprint() != base
    assert command(webhook_url="http://other.test/webhook").fingerprint() != base
    assert command(metadata={}).fingerprint() != base


def test_idempotency_key_is_not_part_of_the_fingerprint() -> None:
    assert command(idempotency_key="a").fingerprint() == command(idempotency_key="b").fingerprint()


def test_amount_is_normalized_to_two_places() -> None:
    assert str(normalize_amount(Decimal(5))) == "5.00"
    assert str(normalize_amount(Decimal("5.1"))) == "5.10"
