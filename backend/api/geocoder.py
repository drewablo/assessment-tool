import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Geography-level geocoder returns FIPS codes alongside coordinates
CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
_GEOCODER_TIMEOUT = 30.0


class GeocoderServiceError(RuntimeError):
    """Raised when the upstream geocoder is unavailable."""



_GEOCODE_CACHE_TTL_SECONDS = int(os.getenv("GEOCODE_CACHE_TTL_SECONDS", "86400"))
_GEOCODE_CACHE_MAX = int(os.getenv("GEOCODE_CACHE_MAX", "1024"))
_GEOCODE_CACHE: dict[str, tuple[datetime, dict | None]] = {}
_MISSING = object()


def _normalize_address(address: str) -> str:
    return " ".join(address.strip().lower().split())


def _cache_get(key: str):
    item = _GEOCODE_CACHE.get(key)
    if not item:
        return _MISSING
    expires_at, payload = item
    if datetime.now(timezone.utc) >= expires_at:
        _GEOCODE_CACHE.pop(key, None)
        return _MISSING
    return payload


def _cache_set(key: str, payload: dict | None) -> None:
    if len(_GEOCODE_CACHE) >= _GEOCODE_CACHE_MAX:
        oldest_key = next(iter(_GEOCODE_CACHE))
        _GEOCODE_CACHE.pop(oldest_key, None)
    _GEOCODE_CACHE[key] = (datetime.now(timezone.utc) + timedelta(seconds=_GEOCODE_CACHE_TTL_SECONDS), payload)

async def geocode_address(address: str) -> Optional[dict]:
    # REVIEW[CACHE]: Geocoding is always live and uncached, so repeated analyses for the same address re-hit the Census geocoder each time.
    """
    Geocode an address using the Census Bureau's free Geocoder API.
    Returns lat/lon plus county and state FIPS codes needed for ACS queries.
    """
    cache_key = _normalize_address(address)
    cached = _cache_get(cache_key)
    if cached is not _MISSING:
        return dict(cached) if cached else None

    params = {
        "address": address,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json",
    }

    async with httpx.AsyncClient(timeout=_GEOCODER_TIMEOUT) as client:
        try:
            response = await client.get(CENSUS_GEOCODER_URL, params=params)
            response.raise_for_status()
            data = response.json()

            matches = data.get("result", {}).get("addressMatches", [])
            if not matches:
                _cache_set(cache_key, None)
                return None

            match = matches[0]
            coords = match["coordinates"]
            geographies = match.get("geographies", {})

            counties = geographies.get("Counties", [{}])
            county_info = counties[0] if counties else {}

            states = geographies.get("States", [{}])
            state_info = states[0] if states else {}

            county_geoid = county_info.get("GEOID", "")  # 5-digit FIPS (state+county)
            state_fips = state_info.get("GEOID", county_geoid[:2] if len(county_geoid) >= 2 else "")

            result = {
                "lat": float(coords["y"]),
                "lon": float(coords["x"]),
                "matched_address": match.get("matchedAddress", address),
                "county_fips": county_geoid,
                "county_name": county_info.get("NAME", "Unknown County"),
                "state_fips": state_fips,
                "state_name": state_info.get("NAME", "Unknown State"),
            }
            _cache_set(cache_key, result)
            return result

        except httpx.TimeoutException:
            logger.warning("Geocoder timeout for address: %s", address)
            raise GeocoderServiceError("Geocoder request timed out")
        except httpx.HTTPStatusError as e:
            logger.error("Geocoder HTTP failure for address %r: %s", address, e)
            raise GeocoderServiceError(f"Geocoder HTTP {e.response.status_code}") from e
        except httpx.RequestError as e:
            logger.error("Geocoding transport error for address %r: %s", address, e)
            raise GeocoderServiceError("Geocoder transport error") from e
        except (KeyError, ValueError) as e:
            logger.error("Geocoding error for address %r: %s", address, e)
            raise GeocoderServiceError("Geocoder response parse error") from e
