import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from app.core.clock import Clock, utc_now
from app.db.models import OutboxEvent, Payment
from app.domain.events import PAYMENT_CREATED, PaymentCreated
from app.domain.payment import Currency, PaymentStatus, normalize_amount

from .ports import UnitOfWorkFactory


@dataclass(frozen=True)
class CreatePaymentCommand:
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str | None
    webhook_url: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def fingerprint(self) -> str:
        """SHA-256 of the normalized body: 100 and 100.00 or reordered metadata keys give the
        same hash, a different amount or URL gives another one."""
        body = {
            "amount": str(normalize_amount(self.amount)),
            "currency": self.currency.value,
            "description": self.description,
            "metadata": self.metadata,
            "webhook_url": self.webhook_url,
        }
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True)
class CreatePaymentResult:
    payment: Payment
    created: bool  # False: the key was used before, this is the original payment


class IdempotencyKeyReusedError(Exception):
    """The same Idempotency-Key came with a different request body."""


class CreatePayment:
    def __init__(self, *, uow_factory: UnitOfWorkFactory, clock: Clock = utc_now) -> None:
        self._uow_factory = uow_factory
        self._clock = clock

    async def __call__(self, cmd: CreatePaymentCommand) -> CreatePaymentResult:
        fingerprint = cmd.fingerprint()
        now = self._clock()
        payment = Payment(
            id=uuid4(),
            idempotency_key=cmd.idempotency_key,
            request_hash=fingerprint,
            amount=normalize_amount(cmd.amount),
            currency=cmd.currency,
            description=cmd.description,
            meta=cmd.metadata,
            status=PaymentStatus.PENDING,
            failure_reason=None,
            webhook_url=cmd.webhook_url,
            created_at=now,
            processed_at=None,
            webhook_delivered_at=None,
        )
        async with self._uow_factory() as uow:
            if await uow.payments.add_if_absent(payment):
                # Same transaction as the payment: either both rows exist or neither does.
                # There is no moment when the payment is saved but its event is lost.
                uow.outbox.add(_payment_created(payment.id, now))
                await uow.commit()
                return CreatePaymentResult(payment=payment, created=True)

            existing = await uow.payments.get_by_idempotency_key(cmd.idempotency_key)
            if existing is None:  # pragma: no cover - the row we conflicted with must exist
                raise RuntimeError("idempotency conflict without an existing payment")
            if existing.request_hash != fingerprint:
                raise IdempotencyKeyReusedError(cmd.idempotency_key)
            return CreatePaymentResult(payment=existing, created=False)


class RequeueStalePayments:
    """Safety net for "the payment is saved, but no message for it will ever be consumed".

    The outbox gets the event into RabbitMQ, yet a message can still land where no code
    picks it up: in the DLQ after a short database outage, or lost if the broker dies while
    dead-lettering it. A payment still pending long after its last event was published gets
    a fresh one; the pauses double and the number of fresh events is limited, because the
    sweep cannot tell a lost message from one still waiting in a long queue. A spare event
    is harmless: processing is idempotent, and the gateway is asked with the payment id as
    the idempotency key.
    """

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        older_than: timedelta,
        max_requeues: int,
        batch_size: int,
        clock: Clock = utc_now,
    ) -> None:
        self._uow_factory = uow_factory
        self._older_than = older_than
        self._max_requeues = max_requeues
        self._batch_size = batch_size
        self._clock = clock

    async def __call__(self) -> int:
        now = self._clock()
        added = 0
        async with self._uow_factory() as uow:
            stale = await uow.payments.find_stale_pending(
                now, self._older_than, self._max_requeues, self._batch_size
            )
            for payment_id in stale:
                added += await uow.outbox.add_unless_waiting(_payment_created(payment_id, now))
            await uow.commit()
        return added


def _payment_created(payment_id: UUID, now: datetime) -> OutboxEvent:
    event = PaymentCreated(event_id=uuid4(), payment_id=payment_id, occurred_at=now)
    return OutboxEvent(
        event_id=event.event_id,
        event_type=PAYMENT_CREATED,
        aggregate_id=payment_id,
        payload=event.model_dump(mode="json"),
        created_at=now,
        next_attempt_at=now,
        attempts=0,
    )


class PaymentQueries:
    def __init__(self, *, uow_factory: UnitOfWorkFactory) -> None:
        self._uow_factory = uow_factory

    async def get(self, payment_id: UUID) -> Payment | None:
        async with self._uow_factory() as uow:
            return await uow.payments.get(payment_id)
