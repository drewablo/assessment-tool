"""Database connection management for PostgreSQL + PostGIS."""

import os
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://feasibility:feasibility@localhost:5432/feasibility",
)

engine = create_async_engine(
    DATABASE_URL,
    echo=os.getenv("SQL_ECHO", "").lower() in ("1", "true"),
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

async_session_factory = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


@asynccontextmanager
async def get_session() -> AsyncSession:
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db():
    """Create all tables and enable PostGIS extension."""
    # Ensure all model modules are imported so metadata includes every table.
    # Without this import, only models imported elsewhere at startup are created.
    from db import models  # noqa: F401

    async with engine.begin() as conn:
        # Prevent concurrent schema initialization when multiple API processes
        # start at the same time (e.g., uvicorn reload/worker startup).
        await conn.execute(text("SELECT pg_advisory_xact_lock(938462015421)"))
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        await conn.run_sync(Base.metadata.create_all)

        # ---------- incremental schema migrations ----------
        # create_all only creates *new* tables; it cannot add columns to
        # existing ones.  We handle that with ADD COLUMN IF NOT EXISTS.
        await conn.execute(text(
            "ALTER TABLE IF EXISTS census_tracts "
            "ADD COLUMN IF NOT EXISTS renter_households_b25070 INTEGER"
        ))
        await conn.execute(text(
            "ALTER TABLE IF EXISTS census_tracts "
            "ADD COLUMN IF NOT EXISTS cost_burdened_renter_households INTEGER"
        ))
        await conn.execute(text(
            "ALTER TABLE IF EXISTS competitors_schools "
            "ADD COLUMN IF NOT EXISTS data_source VARCHAR(20) DEFAULT 'pss'"
        ))
        await conn.execute(text(
            "ALTER TABLE IF EXISTS competitors_schools "
            "ADD COLUMN IF NOT EXISTS also_in_nais BOOLEAN DEFAULT FALSE"
        ))
        await conn.execute(text(
            "ALTER TABLE IF EXISTS competitors_schools "
            "ADD COLUMN IF NOT EXISTS nais_id VARCHAR(30)"
        ))


async def close_db():
    await engine.dispose()
