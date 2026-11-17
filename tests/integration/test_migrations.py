from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
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
