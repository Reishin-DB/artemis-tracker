"""
/api/v1/milestones — mission milestones and timeline.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter

from app.cache import cached

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["milestones"])

MISSION_MILESTONES = [
    {"event": "Launch", "event_name": "Launch", "planned_time": "2026-04-01T22:35:00Z", "planned_ts": "2026-04-01T22:35:00Z", "actual_ts": "2026-04-01T22:35:00Z", "phase": "launch", "description": "SLS liftoff from KSC LC-39B"},
    {"event": "ICPS Separation", "event_name": "ICPS Separation", "planned_time": "2026-04-02T00:35:00Z", "planned_ts": "2026-04-02T00:35:00Z", "actual_ts": "2026-04-02T00:35:00Z", "phase": "earth_orbit", "description": "Interim Cryogenic Propulsion Stage separates"},
    {"event": "Perigee Raise Burn", "event_name": "Perigee Raise Burn", "planned_time": "2026-04-02T08:00:00Z", "planned_ts": "2026-04-02T08:00:00Z", "actual_ts": "2026-04-02T08:12:00Z", "phase": "earth_orbit", "description": "Service module engine burn to raise orbit"},
    {"event": "Trans-Lunar Injection", "event_name": "Trans-Lunar Injection", "planned_time": "2026-04-02T10:00:00Z", "planned_ts": "2026-04-02T10:00:00Z", "actual_ts": "2026-04-02T10:00:00Z", "phase": "transit_out", "description": "ICPS burn sends Orion toward the Moon"},
    {"event": "Outbound Coast", "event_name": "Outbound Coast", "planned_time": "2026-04-02T14:00:00Z", "planned_ts": "2026-04-02T14:00:00Z", "actual_ts": "2026-04-02T14:00:00Z", "phase": "transit_out", "description": "Free-flight trajectory toward the Moon"},
    {"event": "Lunar Flyby", "event_name": "Lunar Flyby", "planned_time": "2026-04-06T12:00:00Z", "planned_ts": "2026-04-06T12:00:00Z", "actual_ts": None, "phase": "lunar_flyby", "description": "Closest approach ~6,400 km from lunar surface"},
    {"event": "Return Coast", "event_name": "Return Coast", "planned_time": "2026-04-07T00:00:00Z", "planned_ts": "2026-04-07T00:00:00Z", "actual_ts": None, "phase": "transit_return", "description": "Free-return trajectory back to Earth"},
    {"event": "Entry Interface", "event_name": "Entry Interface", "planned_time": "2026-04-10T16:00:00Z", "planned_ts": "2026-04-10T16:00:00Z", "actual_ts": None, "phase": "reentry", "description": "Orion enters Earth atmosphere at 25,000 mph"},
    {"event": "Splashdown", "event_name": "Splashdown", "planned_time": "2026-04-10T17:00:00Z", "planned_ts": "2026-04-10T17:00:00Z", "actual_ts": None, "phase": "reentry", "description": "Pacific Ocean recovery"},
]


def _compute_status(ms: dict) -> str:
    now = datetime.now(timezone.utc)
    if ms.get("actual_ts"):
        return "completed"
    planned = ms.get("planned_ts") or ms.get("planned_time")
    if planned:
        try:
            pt = datetime.fromisoformat(planned.replace("Z", "+00:00"))
            if pt <= now:
                return "in_progress"
        except Exception:
            pass
    return "upcoming"


@cached(ttl_seconds=300)
def _fetch_milestones() -> dict[str, Any]:
    # Try DB
    try:
        from app.db import execute_query, get_backend, table
        backend = get_backend()
        if backend in ("postgres", "databricks"):
            sql = f"SELECT event_name, planned_ts, actual_ts, status, phase, description FROM {table('milestones')} ORDER BY planned_ts"
            rows = execute_query(sql)
            if rows:
                return {"milestones": [
                    {
                        "event": r.get("event_name", ""),
                        "event_name": r.get("event_name", ""),
                        "planned_time": r.get("planned_ts"),
                        "planned_ts": r.get("planned_ts"),
                        "actual_ts": r.get("actual_ts"),
                        "status": r.get("status", "upcoming"),
                        "phase": r.get("phase", ""),
                        "description": r.get("description", ""),
                    }
                    for r in rows
                ]}
    except Exception as e:
        logger.warning("DB milestones failed, using hardcoded: %s", e)

    # Fallback: hardcoded real milestones
    return {"milestones": [{**ms, "status": _compute_status(ms)} for ms in MISSION_MILESTONES]}


@router.get("/milestones")
async def get_milestones():
    return _fetch_milestones()
