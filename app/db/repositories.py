from collections.abc import Sequence
from datetime import datetime, timedelta
from types import TracebackType
from typing import Any, Self
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.payment import PaymentStatus

from .models import OutboxEvent, Payment


def _row(entity: Payment) -> dict[str, Any]:
    """Column name -> value of a new ORM object (the attribute "meta" is column "metadata")."""
    mapper = sa.inspect(type(entity))
    return {prop.columns[0].name: getattr(entity, prop.key) for prop in mapper.column_attrs}


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    async def add_if_absent(self, payment: Payment) -> bool:
        """INSERT ... ON CONFLICT (idempotency_key) DO NOTHING.

        A concurrent request with the same key blocks on the unique index until the first
        transaction ends, then inserts nothing. False means the key is already taken.
        """
        stmt = (
            pg_insert(Payment.__table__)
            .values(_row(payment))
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(Payment.__table__.c.id)
        )
        return (await self._s.execute(stmt)).scalar_one_or_none() is not None

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._s.get(Payment, payment_id, populate_existing=True)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        stmt = sa.select(Payment).where(Payment.idempotency_key == key)
        return (await self._s.execute(stmt)).scalar_one_or_none()

    async def find_stale_pending(
        self, now: datetime, older_than: timedelta, max_requeues: int, limit: int
    ) -> Sequence[UUID]:
        """Pending payments whose events are all published, the last one long enough ago.

        "Long enough" is counted from publishing, not from creation (an event that waited
        in the outbox while the broker was down has only just reached the queue), and it
        doubles with every event the payment already has: older_than, then twice that, and
        so on, at most max_requeues fresh events in total.
        """
        of_payment = OutboxEvent.aggregate_id == Payment.id
        events = sa.select(sa.func.count()).where(of_payment).scalar_subquery()
        last_published = (
            sa.select(sa.func.max(OutboxEvent.published_at)).where(of_payment).scalar_subquery()
        )
        waiting = sa.exists().where(of_payment, OutboxEvent.published_at.is_(None))
        since_published = sa.literal(now, sa.DateTime(timezone=True)) - last_published
        stmt = (
            sa.select(Payment.id)
            .where(
                Payment.status == PaymentStatus.PENDING,
                Payment.created_at < now - older_than,
                ~waiting,
                events <= max_requeues,  # the first event plus max_requeues - 1 fresh ones
                sa.extract("epoch", since_published)
                >= older_than.total_seconds() * sa.func.power(2, events - 1),
            )
            .order_by(Payment.created_at)
            .limit(limit)
        )
        return (await self._s.execute(stmt)).scalars().all()

    async def finish(
        self,
        payment_id: UUID,
        status: PaymentStatus,
        failure_reason: str | None,
        processed_at: datetime,
    ) -> bool:
        """pending -> final status, and only from pending.

        The WHERE status = 'pending' makes the transition happen once even if two consumers
        process the same message at the same time: the second UPDATE changes no rows.
        """
        stmt = (
            sa.update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, failure_reason=failure_reason, processed_at=processed_at)
            .execution_options(synchronize_session=False)
        )
        return (await self._s.execute(stmt)).rowcount == 1

    async def mark_webhook_delivered(self, payment_id: UUID, delivered_at: datetime) -> bool:
        stmt = (
            sa.update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=delivered_at)
            .execution_options(synchronize_session=False)
        )
        return (await self._s.execute(stmt)).rowcount == 1


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._s = session

    def add(self, event: OutboxEvent) -> None:
        self._s.add(event)

    async def add_unless_waiting(self, event: OutboxEvent) -> bool:
        """Insert unless the same payment already has an unpublished event.

        The partial unique index allows one waiting event per payment, so two relays that
        both decided to requeue the same payment cannot both insert: the second gets
        "nothing inserted" instead of a duplicate.
        """
        stmt = (
            pg_insert(OutboxEvent)
            .values(
                event_id=event.event_id,
                event_type=event.event_type,
                aggregate_id=event.aggregate_id,
                payload=event.payload,
                created_at=event.created_at,
                next_attempt_at=event.next_attempt_at,
            )
            .on_conflict_do_nothing(
                index_elements=["aggregate_id"],
                index_where=OutboxEvent.published_at.is_(None),
            )
            .returning(OutboxEvent.id)
        )
        return (await self._s.execute(stmt)).scalar_one_or_none() is not None

    async def claim_batch(self, now: datetime, limit: int) -> Sequence[OutboxEvent]:
        """Lock a batch of unpublished events for this transaction.

        SKIP LOCKED: rows another relay instance holds are skipped instead of waited for,
        so several relays share the work without publishing the same row at the same time.
        The locks live until commit, i.e. until the batch is marked as published.
        """
        stmt = (
            sa.select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None), OutboxEvent.next_attempt_at <= now)
            .order_by(OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._s.execute(stmt)).scalars().all()

    async def mark_published(self, ids: Sequence[int], published_at: datetime) -> None:
        if not ids:
            return
        stmt = (
            sa.update(OutboxEvent)
            .where(OutboxEvent.id.in_(ids))
            .values(published_at=published_at)
            .execution_options(synchronize_session=False)
        )
        await self._s.execute(stmt)

    async def mark_failed(self, event_id: int, error: str, next_attempt_at: datetime) -> None:
        stmt = (
            sa.update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                attempts=OutboxEvent.attempts + 1,
                last_error=error[:500],
                next_attempt_at=next_attempt_at,
            )
            .execution_options(synchronize_session=False)
        )
        await self._s.execute(stmt)


class UnitOfWork:
    """One transaction. Whatever is not committed explicitly is rolled back on exit."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def __aenter__(self) -> Self:
        self._session = self._session_factory()
        self.payments = PaymentRepository(self._session)
        self.outbox = OutboxRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # close() rolls back whatever was not committed (releasing row locks) and detaches
        # loaded objects as they are; rollback() would expire them and make them unreadable.
        await self._session.close()

    async def commit(self) -> None:
        await self._session.commit()
