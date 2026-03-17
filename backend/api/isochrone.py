"""
Drive-time isochrone integration via OpenRouteService (ORS).

ORS computes reachability from road-network data, producing an irregular
polygon that hugs highways, respects geographic barriers, and naturally
adapts to urban/rural density differences — a much more human-realistic
catchment boundary than a circle.

API: https://openrouteservice.org/dev/#/api-docs/v2/isochrones/{profile}/post
Free tier: 2,000 requests/day, 40/minute.

Set ORS_API_KEY environment variable to enable. Without it the system
falls back to grade-level-adaptive radii (see GRADE_LEVEL_FALLBACK_RADIUS).
"""

import logging
import math
import os
from typing import Optional

import httpx

from utils import haversine_miles

logger = logging.getLogger(__name__)

ORS_API_KEY = os.getenv("ORS_API_KEY", "")
ORS_ISOCHRONE_URL = "https://api.openrouteservice.org/v2/isochrones/driving-car"
_ORS_TIMEOUT = 12.0  # ORS enforces 40 req/min; short timeout prevents queue buildup

# Fallback radii (miles) when ORS is unavailable. Calibrated to roughly
# match the drive-time defaults for typical suburban road networks.
GRADE_LEVEL_FALLBACK_RADIUS: dict[str, float] = {
    "k5": 8.0,
    "k8": 12.0,
    "high_school": 18.0,
    "k12": 12.0,
}


async def get_isochrone(
    lat: float, lon: float, drive_minutes: int
) -> Optional[dict]:
    """
    Fetch a drive-time isochrone from OpenRouteService.

    Returns a GeoJSON geometry dict (type: "Polygon" or "MultiPolygon")
    or None if ORS is unavailable or the request fails.

    ORS coordinate order is [longitude, latitude] per GeoJSON spec.
    """
    if not ORS_API_KEY:
        return None

    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json, application/geo+json",
    }
    body = {
        "locations": [[lon, lat]],   # ORS uses [lon, lat] order
        "range": [drive_minutes * 60],  # Range in seconds
        "range_type": "time",
    }

    async with httpx.AsyncClient(timeout=_ORS_TIMEOUT) as client:
        try:
            response = await client.post(ORS_ISOCHRONE_URL, json=body, headers=headers)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            if not features:
                return None

            geometry = features[0].get("geometry")
            if not geometry or geometry.get("type") not in ("Polygon", "MultiPolygon"):
                return None

            return geometry

        except httpx.HTTPStatusError as e:
            logger.warning("ORS isochrone HTTP error %s: %s", e.response.status_code, e.request.url)
            return None
        except Exception as e:
            logger.error("ORS isochrone error: %s", e)
            return None


def build_radius_polygon(lat: float, lon: float, radius_km: float, points: int = 72) -> dict:
    """Build a geodesic circle polygon around a center point.

    Returns a GeoJSON Polygon with [lon, lat] coordinates.
    """
    if radius_km <= 0:
        return {"type": "Polygon", "coordinates": [[[lon, lat], [lon, lat], [lon, lat], [lon, lat]]]}

    earth_radius_km = 6371.0088
    angular_distance = radius_km / earth_radius_km
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)

    ring = []
    for i in range(points):
        bearing = 2 * math.pi * i / points
        lat2 = math.asin(
            math.sin(lat1) * math.cos(angular_distance)
            + math.cos(lat1) * math.sin(angular_distance) * math.cos(bearing)
        )
        lon2 = lon1 + math.atan2(
            math.sin(bearing) * math.sin(angular_distance) * math.cos(lat1),
            math.cos(angular_distance) - math.sin(lat1) * math.sin(lat2),
        )
        lon2 = (lon2 + 3 * math.pi) % (2 * math.pi) - math.pi
        ring.append([math.degrees(lon2), math.degrees(lat2)])

    ring.append(ring[0])
    return {"type": "Polygon", "coordinates": [ring]}


def isochrone_effective_radius_miles(
    lat: float, lon: float, polygon_geojson: dict
) -> float:
    """
    Compute the approximate bounding radius of an isochrone polygon in miles:
    the maximum haversine distance from the origin to any vertex on the polygon
    boundary. Used for school pre-filtering (bounding-box optimisation) and as
    the displayed radius when an isochrone is unavailable.
    """
    geom_type = polygon_geojson.get("type", "")
    if geom_type == "Polygon":
        coords = polygon_geojson.get("coordinates", [[]])[0]
    elif geom_type == "MultiPolygon":
        coords = [
            c
            for poly in polygon_geojson.get("coordinates", [])
            for ring in poly
            for c in ring
        ]
    else:
        return 12.0  # Safe fallback

    max_dist = 0.0
    for c in coords:
        dist = haversine_miles(lat, lon, c[1], c[0])  # ORS: [lon, lat]
        max_dist = max(max_dist, dist)

    return round(max_dist, 1) if max_dist > 0 else 12.0
