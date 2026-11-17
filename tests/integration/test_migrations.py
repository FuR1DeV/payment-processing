from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from app.db.models import Base

from .helpers import DATABASE_URL


async def test_migrations_match_the_models() -> None:
    """The migration is written by hand; autogenerate must find nothing left to do."""

    def diff(conn: Connection) -> list[object]:
        return compare_metadata(MigrationContext.configure(conn), Base.metadata)

    engine = create_async_engine(DATABASE_URL)
    try:
        async with engine.connect() as conn:
            assert await conn.run_sync(diff) == []
    finally:
        await engine.dispose()


async def test_constraint_names_follow_the_naming_convention() -> None:
    """Autogenerate does not compare CHECK constraints, so their names are checked here
    (an explicit name without op.f() would get the "ck_payments_" prefix twice)."""
    engine = create_async_engine(DATABASE_URL)
    try:
        async with engine.connect() as conn:
            rows = await conn.execute(
                text(
                    "SELECT conrelid::regclass::text AS tbl, conname FROM pg_constraint"
                    " WHERE conrelid IN ('payments'::regclass, 'outbox'::regclass)"
                )
            )
            names = {(r.tbl, r.conname) for r in rows}
    finally:
        await engine.dispose()
    assert names == {
        ("payments", "pk_payments"),
        ("payments", "uq_payments_idempotency_key"),
        ("payments", "ck_payments_amount_positive"),
        ("payments", "ck_payments_currency"),
        ("payments", "ck_payments_status"),
        ("payments", "ck_payments_processed_at_matches_status"),
        ("outbox", "pk_outbox"),
        ("outbox", "uq_outbox_event_id"),
    }
