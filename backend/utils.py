"""Shared mathematical utilities used across ministry analysis modules."""

import math
from typing import List, Tuple


def piecewise_linear(x: float, segments: List[Tuple[float, float]]) -> float:
    """
    Linearly interpolate between (x, y) breakpoints.
    Clamps to the first/last y value outside the defined range.

    Args:
        x: The input value to score.
        segments: List of (x_breakpoint, y_score) tuples, sorted ascending by x.

    Returns:
        Interpolated float score.
    """
    if x <= segments[0][0]:
        return float(segments[0][1])
    if x >= segments[-1][0]:
        return float(segments[-1][1])
    for i in range(len(segments) - 1):
        x0, y0 = segments[i]
        x1, y1 = segments[i + 1]
        if x0 <= x <= x1:
            t = (x - x0) / (x1 - x0)
            return y0 + t * (y1 - y0)
    return float(segments[-1][1])


def decay_weight(dist_miles: float) -> float:
    """Inverse-distance weight for competitor/tract scoring. Floor at 0.5 miles."""
    return 1.0 / max(0.5, dist_miles) ** 1.5


def saturation_decay_weight(dist_miles: float) -> float:
    """Softer inverse-distance weight for saturation scoring.

    Uses 1/d^1.0 instead of 1/d^1.5 because families consider schools
    across a wider range than proximity-weighted demand signals suggest.
    On the Main Line, in Fairfield County, etc., families routinely compare
    schools 5–10 miles away as direct alternatives.
    """
    return 1.0 / max(0.5, dist_miles) ** 1.0


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance in miles between two lat/lon coordinates."""
    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


_COMPASS_DIRECTIONS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


def bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compass bearing in degrees (0–360) from point 1 to point 2."""
    dlon = math.radians(lon2 - lon1)
    lat1r, lat2r = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2r)
    y = math.cos(lat1r) * math.sin(lat2r) - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def direction_from_bearing(b: float) -> str:
    """Map a bearing in degrees to an 8-point compass direction string."""
    idx = int(((b + 22.5) % 360) // 45)
    return _COMPASS_DIRECTIONS[idx]
