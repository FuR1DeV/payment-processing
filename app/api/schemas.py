import json
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.db.models import Payment
from app.domain.payment import Currency, PaymentStatus

MAX_METADATA_BYTES = 16_384
MAX_WEBHOOK_URL_LENGTH = 2048  # payments.webhook_url is VARCHAR(2048)


def _contains_nul(value: Any) -> bool:
    """The NUL character itself, in keys and values; not the six characters "\\u0000"
    that a client may legitimately have in a string."""
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(_contains_nul(k) or _contains_nul(v) for k, v in value.items())
    if isinstance(value, list):
        return any(_contains_nul(v) for v in value)
    return False


class CreatePaymentRequest(BaseModel):
    """Everything PostgreSQL would reject is rejected here with 422, not with a 500 later."""

    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(
        gt=0,
        max_digits=18,
        decimal_places=2,
        description='A string ("1500.50") or an integer, at most two digits after the point',
        examples=["1500.00"],
    )
    currency: Currency
    description: str | None = Field(default=None, max_length=500, examples=["Loan repayment"])
    metadata: dict[str, Any] = Field(default_factory=dict, examples=[{"order_id": "A-1001"}])
    webhook_url: HttpUrl = Field(examples=["http://webhook-receiver:9000/webhook"])

    @field_validator("amount", mode="before")
    @classmethod
    def _exact_amount(cls, value: Any) -> Any:
        # A JSON number with a fraction is already a float when it gets here, and a float
        # cannot hold every amount: 999999999999999.99 arrives as 1e15. Strings and integers
        # are exact, so only they are accepted.
        if isinstance(value, float | bool):
            raise ValueError('send the amount as a string ("1500.50") or an integer')
        return value

    @field_validator("description")
    @classmethod
    def _no_nul(cls, value: str | None) -> str | None:
        if value is not None and "\x00" in value:
            raise ValueError("must not contain NUL characters")
        return value

    @field_validator("metadata")
    @classmethod
    def _storable_as_jsonb(cls, value: dict[str, Any]) -> dict[str, Any]:
        try:
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
        except ValueError:
            raise ValueError("metadata must not contain NaN or Infinity") from None
        if _contains_nul(value):  # JSONB does not accept the NUL character
            raise ValueError("metadata must not contain NUL characters")
        if len(encoded.encode()) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata must be at most {MAX_METADATA_BYTES} bytes as JSON")
        return value

    @field_validator("webhook_url")
    @classmethod
    def _fits_the_column(cls, value: HttpUrl) -> HttpUrl:
        # Checked after normalization: a non-ASCII URL grows when it is percent-encoded.
        if len(str(value)) > MAX_WEBHOOK_URL_LENGTH:
            raise ValueError(f"must be at most {MAX_WEBHOOK_URL_LENGTH} characters long")
        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def of(cls, p: Payment) -> "PaymentAccepted":
        return cls(payment_id=p.id, status=p.status, created_at=p.created_at)


class PaymentOut(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal = Field(description="Serialized as a string so no precision is lost")
    currency: Currency
    description: str | None
    metadata: dict[str, Any]
    failure_reason: str | None
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None

    @classmethod
    def of(cls, p: Payment) -> "PaymentOut":
        return cls(
            payment_id=p.id,
            status=p.status,
            amount=p.amount,
            currency=p.currency,
            description=p.description,
            metadata=p.meta,
            failure_reason=p.failure_reason,
            idempotency_key=p.idempotency_key,
            webhook_url=p.webhook_url,
            created_at=p.created_at,
            processed_at=p.processed_at,
            webhook_delivered_at=p.webhook_delivered_at,
        )


class ErrorOut(BaseModel):
    code: str
    message: str
