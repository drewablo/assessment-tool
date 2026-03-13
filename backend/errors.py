"""Typed exception hierarchy for the Ministry Feasibility API."""


class FeasibilityError(Exception):
    """Base class for all application-level errors."""


class GeocodingError(FeasibilityError):
    """Address could not be geocoded."""


class ExternalAPIError(FeasibilityError):
    """An external API call failed (Census, ORS, Overpass, etc.)."""


class DataUnavailableError(FeasibilityError):
    """Required data (NCES PSS, CMS, HUD) could not be loaded or is absent."""


class ScoringError(FeasibilityError):
    """An error occurred during feasibility score computation."""
