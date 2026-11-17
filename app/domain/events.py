"""Messages that leave the service: the internal event in RabbitMQ and the client webhook."""

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from .payment import Currency, PaymentStatus

PAYMENT_CREATED = "payment.created"


class PaymentCreated(BaseModel):
    """Body of a message in payments.new.

    Only the id travels: the consumer reads the payment from the database, which stays the
    single source of truth (a message can be delivered twice or late, the row cannot lie).
    """

    model_config = ConfigDict(frozen=True)

    event_id: UUID
    event_type: Literal["payment.created"] = PAYMENT_CREATED
    payment_id: UUID
    occurred_at: datetime


class PaymentWebhook(BaseModel):
    """Body of the webhook sent to the client's webhook_url once the payment is processed.

    Delivery is at-least-once: the client deduplicates by payment_id (one payment has
    exactly one final status, so it gets one notification, possibly repeated).
    """

    model_config = ConfigDict(frozen=True)

    event_type: Literal["payment.succeeded", "payment.failed"]
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any]
    failure_reason: str | None
    created_at: datetime
    processed_at: datetime
