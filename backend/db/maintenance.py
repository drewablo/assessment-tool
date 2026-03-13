"""DB maintenance utilities for data-readiness guardrails."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def backfill_census_centroids(session: AsyncSession, *, state_fips: str | None = None) -> int:
    """Populate missing census tract centroids from tract boundary geometry.

    Uses ST_PointOnSurface(boundary) so the generated point always lies inside the tract polygon.
    """
    sql = """
        UPDATE census_tracts
        SET centroid = ST_PointOnSurface(boundary)
        WHERE centroid IS NULL
          AND boundary IS NOT NULL
    """
    params: dict[str, str] = {}
    if state_fips:
        sql += " AND state_fips = :state_fips"
        params["state_fips"] = state_fips

    result = await session.execute(text(sql), params)
    return int(result.rowcount or 0)
