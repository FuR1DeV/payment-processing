import logging
from dataclasses import dataclass
from uuid import UUID

from app.core.clock import Clock, utc_now
from app.db.models import Payment
from app.domain.events import PaymentWebhook
from app.domain.payment import PaymentStatus

from .ports import ChargeResult, PaymentGateway, UnitOfWorkFactory, WebhookSender

logger = logging.getLogger("payments.processing")

FAILURE_REASON_MAX = 64  # payments.failure_reason is VARCHAR(64)


class PaymentNotFoundError(Exception):
    """The event points to a payment that does not exist. Retrying will not help."""


@dataclass(frozen=True)
class ProcessingReport:
    status: PaymentStatus
    charged: bool  # this run asked the gateway
    webhook_sent: bool  # this run delivered the webhook


class ProcessPayment:
    """What the consumer does with one payments.new message.

    Safe to run any number of times for the same payment, which at-least-once delivery
    requires:
    - the gateway is asked only while the payment is pending, with the payment id as the
      idempotency key, so even two parallel runs end in one charge;
    - pending -> final happens once (conditional UPDATE);
    - a processed payment is not charged again, only its webhook is delivered if that has
      not happened yet.
    Any exception means "try again later" for the caller, except PaymentNotFoundError.
    """

    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        gateway: PaymentGateway,
        webhooks: WebhookSender,
        clock: Clock = utc_now,
    ) -> None:
        self._uow_factory = uow_factory
        self._gateway = gateway
        self._webhooks = webhooks
        self._clock = clock

    async def __call__(self, payment_id: UUID) -> ProcessingReport:
        payment = await self._load(payment_id)

        charged = False
        if payment.status is PaymentStatus.PENDING:
            # No transaction and no pooled connection is held during the 2-5 s call.
            result = await self._gateway.charge(payment.id, payment.amount, payment.currency)
            payment = await self._finish(payment.id, result)
            charged = True

        if payment.webhook_delivered_at is not None:
            return ProcessingReport(payment.status, charged=charged, webhook_sent=False)

        await self._webhooks.send(payment.webhook_url, build_webhook(payment))
        async with self._uow_factory() as uow:
            await uow.payments.mark_webhook_delivered(payment.id, self._clock())
            await uow.commit()
        return ProcessingReport(payment.status, charged=charged, webhook_sent=True)

    async def _load(self, payment_id: UUID) -> Payment:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    async def _finish(self, payment_id: UUID, result: ChargeResult) -> Payment:
        status = PaymentStatus.SUCCEEDED if result.approved else PaymentStatus.FAILED
        # A gateway may send a longer reason than the column holds; the decision must still
        # be stored, so the reason is cut rather than the UPDATE failing on every retry.
        reason = result.decline_reason[:FAILURE_REASON_MAX] if result.decline_reason else None
        async with self._uow_factory() as uow:
            changed = await uow.payments.finish(payment_id, status, reason, self._clock())
            await uow.commit()
            # Our UPDATE or a parallel run's one settled the payment: continue with the winner.
            payment = await uow.payments.get(payment_id)
        if payment is None:  # pragma: no cover - payments are never deleted
            raise PaymentNotFoundError(payment_id)
        if not changed:
            logger.info("payment_already_finished", extra={"status": payment.status.value})
        return payment


def build_webhook(payment: Payment) -> PaymentWebhook:
    if payment.processed_at is None:  # pragma: no cover - guaranteed by a CHECK constraint
        raise ValueError("webhook for a payment that is not processed")
    return PaymentWebhook(
        event_type=f"payment.{payment.status.value}",  # type: ignore[arg-type]
        payment_id=payment.id,
        status=payment.status,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.meta,
        failure_reason=payment.failure_reason,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )
