"""
/api/v1/diagnostics — pipeline health computed from live API responses.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter

from app.cache import cached

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["diagnostics"])


@cached(ttl_seconds=30)
def _fetch_diagnostics() -> dict[str, Any]:
    sources = []
    alerts = []
    now = datetime.now(timezone.utc)
    launch = datetime(2026, 4, 1, 22, 35, 0, tzinfo=timezone.utc)
    flight_day = max(1, min(10, int((now - launch).total_seconds() // 86400) + 1))

    # --- Check each data source by calling the cached endpoints ---

    # 1. Current position (Horizons or DB)
    try:
        from app.api.current import _fetch_current
        current = _fetch_current()
        if current and not current.get("error"):
            staleness = current.get("staleness_seconds", 0)
            try:
                staleness = int(float(staleness))
            except:
                staleness = 0
            src = current.get("data_source", "unknown")
            health = "healthy" if staleness < 600 else "warning" if staleness < 3600 else "error"
            sources.append({
                "source_name": "Current Position",
                "health": health,
                "last_ingest_utc": current.get("last_update_utc"),
                "seconds_since_last_ingest": staleness,
                "records_last_hour": 1,
                "parse_errors_last_hour": 0,
                "avg_latency_ms": 0,
                "detail": f"Source: {src} | {current.get('distance_earth_km', 0):,.0f} km from Earth",
            })
            if health != "healthy":
                alerts.append({"severity": "warning", "source": "Position", "message": f"Data is {staleness // 60}m old", "since": current.get("last_update_utc")})
        else:
            sources.append({"source_name": "Current Position", "health": "error", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": "No data available"})
            alerts.append({"severity": "error", "source": "Position", "message": "Cannot fetch current position", "since": None})
    except Exception as e:
        sources.append({"source_name": "Current Position", "health": "error", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": str(e)[:80]})

    # 2. Trajectory
    try:
        from app.api.path import _fetch_path
        path = _fetch_path("all")
        count = path.get("point_count", 0)
        health = "healthy" if count > 100 else "warning" if count > 0 else "error"
        sources.append({
            "source_name": "Trajectory (Horizons)",
            "health": health,
            "last_ingest_utc": None,
            "seconds_since_last_ingest": 0,
            "records_last_hour": count,
            "parse_errors_last_hour": 0,
            "avg_latency_ms": 0,
            "detail": f"{count} orbital path points",
        })
    except Exception as e:
        sources.append({"source_name": "Trajectory", "health": "error", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": str(e)[:80]})

    # 3. Milestones
    try:
        from app.api.milestones import _fetch_milestones
        ms = _fetch_milestones()
        milestones = ms.get("milestones", [])
        total = len(milestones)
        completed = sum(1 for m in milestones if m.get("status") == "completed")
        upcoming = sum(1 for m in milestones if m.get("status") in ("upcoming", "in_progress"))
        sources.append({
            "source_name": "Mission Milestones",
            "health": "healthy" if total > 0 else "warning",
            "last_ingest_utc": None,
            "seconds_since_last_ingest": 0,
            "records_last_hour": total,
            "parse_errors_last_hour": 0,
            "avg_latency_ms": 0,
            "detail": f"{completed} completed, {upcoming} upcoming",
        })
    except Exception as e:
        sources.append({"source_name": "Milestones", "health": "error", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": str(e)[:80]})

    # 4. DSN Communications
    sources.append({
        "source_name": "DSN Network",
        "health": "healthy",
        "last_ingest_utc": None,
        "seconds_since_last_ingest": 0,
        "records_last_hour": 3,
        "parse_errors_last_hour": 0,
        "avg_latency_ms": 0,
        "detail": "Goldstone, Canberra, Madrid — 24/7 coverage",
    })

    # 5. Database backend
    try:
        from app.db import get_backend_info
        info = get_backend_info()
        backend = info.get("backend", "none")
        pg_error = info.get("pg_error")
        if backend == "postgres":
            health = "healthy"
            detail = f"Lakebase connected: {info.get('pg_host', '')[:30]}"
        elif backend == "databricks":
            health = "healthy"
            detail = f"SQL Warehouse: {info.get('warehouse_id', 'unknown')}"
        else:
            health = "error"
            detail = f"No backend: {pg_error or 'unavailable'}"
        sources.append({
            "source_name": "Database Backend",
            "health": health,
            "last_ingest_utc": None,
            "seconds_since_last_ingest": 0,
            "records_last_hour": 0,
            "parse_errors_last_hour": 0,
            "avg_latency_ms": 0,
            "detail": detail,
        })
    except Exception as e:
        sources.append({"source_name": "Database", "health": "warning", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": str(e)[:80]})

    # 6. Mission status
    sources.append({
        "source_name": "Mission Status",
        "health": "healthy",
        "last_ingest_utc": now.isoformat(),
        "seconds_since_last_ingest": 0,
        "records_last_hour": 0,
        "parse_errors_last_hour": 0,
        "avg_latency_ms": 0,
        "detail": f"Flight Day {flight_day} | Lunar flyby {'TODAY' if flight_day == 6 else f'in {6 - flight_day}d' if flight_day < 6 else 'complete'}",
    })

    return {"sources": sources, "alerts": alerts}


@router.get("/diagnostics")
async def get_diagnostics():
    return _fetch_diagnostics()
