from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone

from models.schemas import AnalysisRequest, AnalysisResponse


SNAPSHOT_SCHEMA_VERSION = os.getenv("ANALYSIS_CACHE_SCHEMA_VERSION", "2026-03-senior-metrics-v2")


def snapshot_key(request: AnalysisRequest) -> str:
    payload = json.dumps({
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "request": request.model_dump(mode="json"),
    }, sort_keys=True)
    digest = hashlib.sha256(payload.encode()).hexdigest()[:20]
    return f"snapshot:{digest}"


def freeze_snapshot(result: AnalysisResponse) -> dict:
    return {
        "frozen_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "analysis": result.model_dump(mode="json"),
    }


def thaw_snapshot(payload: str | bytes | dict | None) -> AnalysisResponse | None:
    if payload is None:
        return None
    if isinstance(payload, (str, bytes)):
        body = json.loads(payload)
    else:
        body = payload
    analysis = body.get("analysis") if isinstance(body, dict) else None
    if not analysis:
        return None
    return AnalysisResponse.model_validate(analysis)
