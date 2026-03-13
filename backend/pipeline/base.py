"""Base utilities for data ingestion pipelines."""

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import PipelineRun

logger = logging.getLogger("pipeline")


def _utcnow_db_naive() -> datetime:
    """Return naive UTC datetime for TIMESTAMP WITHOUT TIME ZONE columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def start_pipeline_run(session: AsyncSession, pipeline_name: str) -> PipelineRun:
    """Log the start of a pipeline run."""
    run = PipelineRun(
        pipeline_name=pipeline_name,
        status="running",
        started_at=_utcnow_db_naive(),
    )
    session.add(run)
    await session.flush()
    return run


async def finish_pipeline_run(
    session: AsyncSession,
    run: PipelineRun,
    status: str = "success",
    records_processed: int = 0,
    records_inserted: int = 0,
    records_updated: int = 0,
    error_message: str | None = None,
    metadata: dict | None = None,
):
    """Update pipeline run with completion status."""
    run.status = status
    run.finished_at = _utcnow_db_naive()
    run.records_processed = records_processed
    run.records_inserted = records_inserted
    run.records_updated = records_updated
    run.error_message = error_message
    run.metadata_json = metadata
    await session.flush()


async def get_latest_run(session: AsyncSession, pipeline_name: str) -> PipelineRun | None:
    """Get the most recent successful run of a pipeline."""
    stmt = (
        select(PipelineRun)
        .where(
            PipelineRun.pipeline_name == pipeline_name,
            PipelineRun.status == "success",
        )
        .order_by(PipelineRun.finished_at.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()
