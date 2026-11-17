import json
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from app.api.schemas import MAX_METADATA_BYTES, CreatePaymentRequest


def body(**overrides: Any) -> dict[str, Any]:
    return {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Loan repayment",
        "metadata": {"order_id": "A-1"},
        "webhook_url": "https://client.test/webhook",
    } | overrides


@pytest.mark.parametrize("amount", ["1500.00", "1500", 1500, "1500.5", "0.01"])
def test_valid_amounts(amount: object) -> None:
    parsed = CreatePaymentRequest.model_validate(body(amount=amount))
    assert parsed.amount == Decimal(str(amount))


@pytest.mark.parametrize("amount", ["0", 0, "-1", "10.001", "abc", "NaN", "Infinity", None, True])
def test_invalid_amounts(amount: object) -> None:
    with pytest.raises(ValidationError):
        CreatePaymentRequest.model_validate(body(amount=amount))


@pytest.mark.parametrize("raw", ["1500.5", "1500.0", "999999999999999.99"])
def test_json_number_with_a_fraction_is_rejected(raw: str) -> None:
    """It would arrive as a float: 999999999999999.99 would silently become 1e15."""
    payload = json.dumps(body()).replace('"1500.00"', raw)
    with pytest.raises(ValidationError, match="as a string"):
        CreatePaymentRequest.model_validate(json.loads(payload))


def test_large_amount_as_a_string_is_exact() -> None:
    parsed = CreatePaymentRequest.model_validate(body(amount="999999999999999.99"))
    assert str(parsed.amount) == "999999999999999.99"


@pytest.mark.parametrize(
    "metadata",
    [{"x": float("nan")}, {"x": float("inf")}, {"x": "a\x00b"}, {"k\x00": 1}],
)
def test_metadata_postgres_cannot_store_is_rejected(metadata: dict[str, object]) -> None:
    with pytest.raises(ValidationError, match="metadata"):
        CreatePaymentRequest.model_validate(body(metadata=metadata))


def test_backslash_u0000_text_is_not_a_nul_character() -> None:
    """Six ordinary characters, e.g. in a Windows path: JSONB stores them fine."""
    parsed = CreatePaymentRequest.model_validate(body(metadata={"path": "C:\\u0000dir"}))
    assert parsed.metadata == {"path": "C:\\u0000dir"}


def test_nul_in_description_is_rejected() -> None:
    with pytest.raises(ValidationError, match="NUL"):
        CreatePaymentRequest.model_validate(body(description="a\x00b"))


def test_webhook_url_length_is_checked_after_normalization() -> None:
    # 1000 Cyrillic letters are 6000 characters once percent-encoded.
    with pytest.raises(ValidationError, match="2048"):
        CreatePaymentRequest.model_validate(body(webhook_url="https://client.test/" + "я" * 1000))


def test_amount_has_at_most_16_integer_digits() -> None:
    CreatePaymentRequest.model_validate(body(amount="9999999999999999.99"))
    with pytest.raises(ValidationError):
        CreatePaymentRequest.model_validate(body(amount="99999999999999999.99"))


@pytest.mark.parametrize("currency", ["RUB", "USD", "EUR"])
def test_supported_currencies(currency: str) -> None:
    assert CreatePaymentRequest.model_validate(body(currency=currency)).currency == currency


@pytest.mark.parametrize("currency", ["rub", "GBP", "", None])
def test_unsupported_currencies(currency: object) -> None:
    with pytest.raises(ValidationError):
        CreatePaymentRequest.model_validate(body(currency=currency))


@pytest.mark.parametrize(
    "url", ["ftp://client.test/hook", "not a url", "file:///etc/passwd", "javascript:alert(1)"]
)
def test_webhook_url_must_be_http(url: str) -> None:
    with pytest.raises(ValidationError):
        CreatePaymentRequest.model_validate(body(webhook_url=url))


def test_metadata_defaults_to_empty_and_description_is_optional() -> None:
    parsed = CreatePaymentRequest.model_validate(
        {"amount": "1", "currency": "USD", "webhook_url": "http://client.test/hook"}
    )
    assert parsed.metadata == {}
    assert parsed.description is None


def test_metadata_size_is_limited() -> None:
    with pytest.raises(ValidationError, match="metadata"):
        CreatePaymentRequest.model_validate(body(metadata={"blob": "x" * MAX_METADATA_BYTES}))


def test_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        CreatePaymentRequest.model_validate(body(status="succeeded"))
