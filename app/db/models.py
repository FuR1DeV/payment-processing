"""ORM models. The migration in migrations/versions is written by hand; a test checks that
it matches these models (tests/integration/test_migrations.py)."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.domain.payment import Currency, PaymentStatus

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = sa.MetaData(naming_convention=NAMING_CONVENTION)
    type_annotation_map = {  # noqa: RUF012
        datetime: sa.DateTime(timezone=True),
        dict[str, Any]: JSONB,
    }


def _string_enum(enum: type[StrEnum], name: str) -> sa.Enum:
    # VARCHAR + CHECK instead of a native PostgreSQL enum: a new value later is a plain
    # migration that replaces the constraint, without ALTER TYPE.
    return sa.Enum(
        enum,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=16,
        values_callable=lambda members: [m.value for m in members],
        validate_strings=True,
    )


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(sa.String(255), unique=True)
    # SHA-256 of the normalized request body: the same key with another body is an error.
    request_hash: Mapped[str] = mapped_column(sa.String(64))
    amount: Mapped[Decimal] = mapped_column(sa.Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(_string_enum(Currency, "currency"))
    description: Mapped[str | None] = mapped_column(sa.String(500))
    # The attribute cannot be called "metadata": the name is taken by SQLAlchemy.
    meta: Mapped[dict[str, Any]] = mapped_column("metadata")
    status: Mapped[PaymentStatus] = mapped_column(_string_enum(PaymentStatus, "status"))
    failure_reason: Mapped[str | None] = mapped_column(sa.String(64))
    webhook_url: Mapped[str] = mapped_column(sa.String(2048))
    created_at: Mapped[datetime]
    processed_at: Mapped[datetime | None]
    webhook_delivered_at: Mapped[datetime | None]

    __table_args__ = (
        sa.CheckConstraint("amount > 0", name="amount_positive"),
        # A final status always has a processing time and pending never has one.
        sa.CheckConstraint(
            "(status = 'pending') = (processed_at IS NULL)", name="processed_at_matches_status"
        ),
        # For the stale-payment sweep: pending payments are few, processed ones are many.
        sa.Index(
            "ix_payments_pending_created_at",
            "created_at",
            postgresql_where=sa.text("status = 'pending'"),
        ),
    )


class OutboxEvent(Base):
    """An event waiting to be published to RabbitMQ.

    Written in the same transaction as the business change, published later by the relay.
    """

    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(sa.BigInteger, sa.Identity(), primary_key=True)
    # Becomes the AMQP message_id: lets a consumer recognise a repeated delivery.
    event_id: Mapped[UUID] = mapped_column(unique=True)
    event_type: Mapped[str] = mapped_column(sa.String(64))
    aggregate_id: Mapped[UUID] = mapped_column(index=True)
    payload: Mapped[dict[str, Any]]
    created_at: Mapped[datetime]
    published_at: Mapped[datetime | None]
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    next_attempt_at: Mapped[datetime]
    last_error: Mapped[str | None] = mapped_column(sa.String(500))

    __table_args__ = (
        # The relay only reads unpublished rows; a partial index keeps that lookup small
        # however many published rows pile up.
        sa.Index(
            "ix_outbox_unpublished",
            "id",
            postgresql_where=sa.text("published_at IS NULL"),
        ),
        # At most one event waiting to be published per payment: the stale-payment sweep
        # in two relays at once cannot add two.
        sa.Index(
            "uq_outbox_one_waiting_per_aggregate",
            "aggregate_id",
            unique=True,
            postgresql_where=sa.text("published_at IS NULL"),
        ),
    )
