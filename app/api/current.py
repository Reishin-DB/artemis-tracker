"""
/api/v1/current — real-time mission status.
"""

import logging
import math
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from fastapi import APIRouter

from app.cache import cached

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["current"])

_last_good: Optional[dict[str, Any]] = None


def _parse_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _parse_horizons_vector(command: str, start: str, stop: str) -> Optional[tuple]:
    try:
        resp = requests.get("https://ssd.jpl.nasa.gov/api/horizons.api", params={
            "format": "text", "COMMAND": f"'{command}'", "EPHEM_TYPE": "'VECTORS'",
            "CENTER": "'500@399'", "START_TIME": f"'{start}'",
            "STOP_TIME": f"'{stop}'", "STEP_SIZE": "'5 MINUTES'",
            "REF_PLANE": "'FRAME'", "VEC_TABLE": "'2'",
            "MAKE_EPHEM": "'YES'", "OBJ_DATA": "'NO'", "CSV_FORMAT": "'YES'"
        }, timeout=15)
        in_data = False
        last_row = None
        for line in resp.text.split("\n"):
            line = line.strip()
            if "$$SOE" in line: in_data = True; continue
            if "$$EOE" in line: break
            if in_data and line:
                parts = [p.strip().rstrip(",") for p in line.split(",")]
                if len(parts) >= 8: last_row = parts
        if last_row:
            return tuple(float(last_row[i]) for i in range(2, 8))
    except Exception as e:
        logger.warning("Horizons query for %s failed: %s", command, e)
    return None


def _fetch_from_horizons() -> dict[str, Any]:
    global _last_good
    LAUNCH = datetime(2026, 4, 1, 22, 35, 0, tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    start = (now - __import__('datetime').timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M")
    stop = now.strftime("%Y-%m-%d %H:%M")

    orion = _parse_horizons_vector("-1024", start, stop)
    if not orion:
        if _last_good:
            return {**_last_good, "stale": True}
        return {"error": "No data available", "stale": True}

    x, y, z, vx, vy, vz = orion
    dist_e = math.sqrt(x**2 + y**2 + z**2)
    speed = math.sqrt(vx**2 + vy**2 + vz**2) * 3600

    dist_m = 0.0
    moon = _parse_horizons_vector("301", start, stop)
    if moon:
        dist_m = math.sqrt((x - moon[0])**2 + (y - moon[1])**2 + (z - moon[2])**2)

    elapsed = (now - LAUNCH).total_seconds()
    d, h, m = int(elapsed // 86400), int((elapsed % 86400) // 3600), int((elapsed % 3600) // 60)

    # Phase based on mission timeline (flyby Apr 6 12:00 UTC, return starts Apr 7)
    from datetime import datetime, timezone
    FLYBY_TIME = datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc)
    RETURN_START = datetime(2026, 4, 7, 0, 0, 0, tzinfo=timezone.utc)
    ENTRY_TIME = datetime(2026, 4, 10, 16, 0, 0, tzinfo=timezone.utc)
    now_utc = datetime.now(timezone.utc)

    if dist_e < 10000: phase = "near_earth"
    elif now_utc < FLYBY_TIME: phase = "transit_out"
    elif now_utc < RETURN_START: phase = "lunar_flyby"
    elif now_utc < ENTRY_TIME: phase = "transit_return"
    else: phase = "reentry"

    result = {
        "last_update_utc": now.isoformat(),
        "mission_elapsed_s": elapsed,
        "mission_elapsed_display": f"{d}d {h}h {m}m",
        "current_phase": phase, "phase": phase,
        "last_milestone": {"transit_out": "Outbound Coast", "lunar_flyby": "Lunar Flyby", "transit_return": "Return Coast", "reentry": "Entry Interface"}.get(phase, "Outbound Coast"),
        "distance_earth_km": dist_e, "distance_earth_miles": dist_e * 0.621371,
        "distance_moon_km": dist_m, "distance_moon_miles": dist_m * 0.621371,
        "speed_km_h": speed, "speed_mph": speed * 0.621371,
        "position": {"x_km": x, "y_km": y, "z_km": z},
        "velocity": {"vx_km_s": vx, "vy_km_s": vy, "vz_km_s": vz},
        "data_source": "horizons_live", "staleness_seconds": 0, "stale": False,
    }
    _last_good = result
    return result


def _fetch_from_db() -> Optional[dict[str, Any]]:
    """Try fetching from database. Returns None if anything fails."""
    try:
        from app.db import execute_query_single, get_backend, table
        backend = get_backend()
        if backend == "postgres":
            sql = f"SELECT *, EXTRACT(EPOCH FROM (NOW() - updated_at))::int AS staleness_seconds FROM {table('current_status')} WHERE id = 1"
        elif backend == "databricks":
            sql = f"SELECT *, CAST(TIMESTAMPDIFF(SECOND, updated_at, current_timestamp()) AS INT) AS staleness_seconds FROM {table('current_status')} WHERE id = 1"
        else:
            return None
        row = execute_query_single(sql)
        if not row or row.get("error"):
            return None

        staleness = _parse_float(row.get("staleness_seconds"), 0.0)
        return {
            "last_update_utc": row.get("last_update_utc"),
            "mission_elapsed_s": _parse_float(row.get("mission_elapsed_s")),
            "mission_elapsed_display": row.get("mission_elapsed_display", ""),
            "current_phase": row.get("current_phase", ""),
            "phase": row.get("current_phase", ""),
            "last_milestone": row.get("last_milestone", ""),
            "distance_earth_km": _parse_float(row.get("distance_earth_km")),
            "distance_earth_miles": _parse_float(row.get("distance_earth_miles")),
            "distance_moon_km": _parse_float(row.get("distance_moon_km")),
            "distance_moon_miles": _parse_float(row.get("distance_moon_miles")),
            "speed_km_h": _parse_float(row.get("speed_km_h")),
            "speed_mph": _parse_float(row.get("speed_mph")),
            "position": {
                "x_km": _parse_float(row.get("x_km")),
                "y_km": _parse_float(row.get("y_km")),
                "z_km": _parse_float(row.get("z_km")),
            },
            "velocity": {
                "vx_km_s": _parse_float(row.get("vx_km_s")),
                "vy_km_s": _parse_float(row.get("vy_km_s")),
                "vz_km_s": _parse_float(row.get("vz_km_s")),
            },
            "data_source": row.get("data_source", ""),
            "staleness_seconds": staleness,
            "stale": staleness > 600,
        }
    except Exception as e:
        logger.warning("DB fetch failed, will use Horizons: %s", e)
        return None


@cached(ttl_seconds=30)
def _fetch_current() -> dict[str, Any]:
    # Try DB first
    result = _fetch_from_db()
    if result:
        # If DB data is stale (>10 min), prefer live Horizons
        staleness = result.get("staleness_seconds", 0)
        try:
            staleness = float(staleness)
        except:
            staleness = 0
        if staleness > 600:
            live = _fetch_from_horizons()
            if live and not live.get("error"):
                return live
        return result
    return _fetch_from_horizons()


@router.get("/current")
async def get_current_status():
    return _fetch_current()
