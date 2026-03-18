from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from statistics import pstdev
from typing import Iterable


@dataclass(frozen=True)
class HistoricalValue:
    year: int
    value: float


@dataclass(frozen=True)
class ProjectionPoint:
    year: int
    value: float
    projected: bool
    lower_bound: float | None = None
    upper_bound: float | None = None


@dataclass(frozen=True)
class ProjectionConfidence:
    band: str
    volatility: float


@dataclass(frozen=True)
class ProjectionEnvelope:
    points: list[ProjectionPoint]
    confidence: ProjectionConfidence


def _clean_series(series: Iterable[HistoricalValue]) -> list[HistoricalValue]:
    cleaned = [point for point in series if point.value is not None and isfinite(point.value) and point.value >= 0]
    return sorted(cleaned, key=lambda point: point.year)


def compound_annual_growth_rate(series: Iterable[HistoricalValue]) -> float:
    cleaned = _clean_series(series)
    if len(cleaned) < 2:
        return 0.0

    start = cleaned[0]
    end = cleaned[-1]
    periods = max(1, end.year - start.year)

    if start.value <= 0:
        return 0.0

    return (end.value / start.value) ** (1 / periods) - 1


def projection_confidence(series: Iterable[HistoricalValue]) -> ProjectionConfidence:
    cleaned = _clean_series(series)
    if len(cleaned) < 3:
        return ProjectionConfidence(band="low", volatility=0.0)

    changes: list[float] = []
    for previous, current in zip(cleaned, cleaned[1:]):
        if previous.value <= 0:
            continue
        changes.append((current.value - previous.value) / previous.value)

    if not changes:
        return ProjectionConfidence(band="low", volatility=0.0)

    volatility = pstdev(changes)
    if volatility < 0.03:
        band = "high"
    elif volatility < 0.08:
        band = "medium"
    else:
        band = "low"
    return ProjectionConfidence(band=band, volatility=round(volatility, 4))


def build_cagr_projection(series: Iterable[HistoricalValue], projection_years: Iterable[int]) -> list[ProjectionPoint]:
    cleaned = _clean_series(series)
    if not cleaned:
        return []

    cagr = compound_annual_growth_rate(cleaned)
    baseline_year = cleaned[-1].year
    baseline_value = cleaned[-1].value

    points = [ProjectionPoint(year=point.year, value=round(point.value, 2), projected=False) for point in cleaned]
    for year in sorted(set(projection_years)):
        if year <= baseline_year:
            continue
        delta = year - baseline_year
        value = baseline_value * ((1 + cagr) ** delta)
        points.append(ProjectionPoint(year=year, value=round(max(0.0, value), 2), projected=True))

    return points


def build_projection_envelope(series: Iterable[HistoricalValue], projection_years: Iterable[int]) -> ProjectionEnvelope:
    cleaned = _clean_series(series)
    confidence = projection_confidence(cleaned)
    points = build_cagr_projection(cleaned, projection_years)
    if not points:
        return ProjectionEnvelope(points=[], confidence=confidence)

    spread = max(0.03, confidence.volatility if confidence.volatility > 0 else 0.05)
    enriched: list[ProjectionPoint] = []
    for point in points:
        if not point.projected:
            enriched.append(point)
            continue
        margin = max(point.value * spread, 1.0)
        enriched.append(
            ProjectionPoint(
                year=point.year,
                value=point.value,
                projected=True,
                lower_bound=round(max(0.0, point.value - margin), 2),
                upper_bound=round(point.value + margin, 2),
            )
        )
    return ProjectionEnvelope(points=enriched, confidence=confidence)


def project_surviving_cohort(base_population: int, annual_survival_rate: float, years_out: int) -> int:
    if base_population <= 0 or annual_survival_rate <= 0:
        return 0
    return max(0, round(base_population * (annual_survival_rate ** years_out)))
