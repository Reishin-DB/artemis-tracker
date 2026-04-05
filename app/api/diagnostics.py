"""
/api/v1/diagnostics — pipeline health.
"""

import logging
from typing import Any

from fastapi import APIRouter

from app.cache import cached

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["diagnostics"])


def _safe_int(val: Any, default: int = 0) -> int:
    if val is None: return default
    try: return int(val)
    except: return default


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None: return default
    try: return float(val)
    except: return default


@cached(ttl_seconds=30)
def _fetch_diagnostics() -> dict[str, Any]:
    sources = []
    alerts = []

    try:
        from app.db import execute_query_single, get_backend, table
        backend = get_backend()

        if backend in ("postgres", "databricks"):
            # Trajectory
            try:
                if backend == "postgres":
                    row = execute_query_single(f"SELECT COUNT(*) AS total_rows, MAX(epoch_utc) AS latest_epoch, EXTRACT(EPOCH FROM (NOW() - MAX(epoch_utc)))::int AS lag FROM {table('trajectory_history')}")
                else:
                    row = execute_query_single(f"SELECT COUNT(*) AS total_rows, MAX(epoch_utc) AS latest_epoch, CAST(TIMESTAMPDIFF(SECOND, MAX(epoch_utc), current_timestamp()) AS INT) AS lag FROM {table('trajectory_history')}")
                total = _safe_int(row.get("total_rows"))
                lag = _safe_int(row.get("lag"))
                health = "healthy" if lag < 7200 else "warning" if lag < 86400 else "error"
                sources.append({"source_name": "JPL Horizons", "health": health, "last_ingest_utc": row.get("latest_epoch"), "seconds_since_last_ingest": lag, "records_last_hour": total, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": f"{total} trajectory points"})
            except Exception as e:
                sources.append({"source_name": "JPL Horizons", "health": "error", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": f"Query error: {str(e)[:80]}"})

            # Milestones
            try:
                row = execute_query_single(f"SELECT COUNT(*) AS cnt FROM {table('milestones')}")
                total = _safe_int(row.get("cnt"))
                sources.append({"source_name": "Mission Milestones", "health": "healthy" if total > 0 else "warning", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": total, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": f"{total} milestones"})
            except Exception as e:
                sources.append({"source_name": "Mission Milestones", "health": "warning", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": f"Query error: {str(e)[:80]}"})

            # Media
            try:
                row = execute_query_single(f"SELECT COUNT(*) AS cnt FROM {table('media_catalog')}")
                total = _safe_int(row.get("cnt"))
                sources.append({"source_name": "NASA Media API", "health": "healthy" if total > 0 else "warning", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": total, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": f"{total} media items"})
            except Exception as e:
                sources.append({"source_name": "NASA Media API", "health": "warning", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": f"Query error: {str(e)[:80]}"})

            sources.append({"source_name": "Database Backend", "health": "healthy", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": f"Connected via {backend}"})
        else:
            sources.append({"source_name": "JPL Horizons API", "health": "healthy", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": "Live queries"})
            sources.append({"source_name": "Database", "health": "warning", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": "No database connected"})
            alerts.append({"severity": "warning", "source": "Database", "message": "No database — using live NASA APIs", "since": None})

    except Exception as e:
        logger.exception("Diagnostics failed entirely")
        sources.append({"source_name": "System", "health": "error", "last_ingest_utc": None, "seconds_since_last_ingest": 0, "records_last_hour": 0, "parse_errors_last_hour": 0, "avg_latency_ms": 0, "detail": f"Error: {str(e)[:100]}"})
        alerts.append({"severity": "error", "source": "System", "message": str(e)[:100], "since": None})

    return {"sources": sources, "alerts": alerts}


@router.get("/diagnostics")
async def get_diagnostics():
    return _fetch_diagnostics()
