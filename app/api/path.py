"""
/api/v1/path — trajectory history for 3-D orbit visualization.
"""

import logging
import math
from typing import Any

import requests
from fastapi import APIRouter, Query

from app.cache import cached

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["path"])


def _parse_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _fetch_path_from_horizons() -> dict[str, Any]:
    """Fetch full trajectory from Horizons: launch to splashdown, 15-min steps."""
    try:
        resp = requests.get("https://ssd.jpl.nasa.gov/api/horizons.api", params={
            "format": "text", "COMMAND": "'-1024'", "EPHEM_TYPE": "'VECTORS'",
            "CENTER": "'500@399'", "START_TIME": "'2026-04-02 02:00'",
            "STOP_TIME": "'2026-04-10 18:00'",
            "STEP_SIZE": "'15 MINUTES'",
            "REF_PLANE": "'FRAME'", "VEC_TABLE": "'2'",
            "MAKE_EPHEM": "'YES'", "OBJ_DATA": "'NO'", "CSV_FORMAT": "'YES'"
        }, timeout=45)
        points = []
        in_data = False
        for line in resp.text.split("\n"):
            line = line.strip()
            if "$$SOE" in line:
                in_data = True
                continue
            if "$$EOE" in line:
                break
            if not in_data or not line:
                continue
            parts = [p.strip().rstrip(",") for p in line.split(",")]
            if len(parts) >= 8:
                try:
                    x, y, z = float(parts[2]), float(parts[3]), float(parts[4])
                    dist_e = math.sqrt(x**2 + y**2 + z**2)
                    speed = math.sqrt(float(parts[5])**2 + float(parts[6])**2 + float(parts[7])**2) * 3600
                    points.append({
                        "epoch_utc": parts[1].strip().replace("A.D. ", ""),
                        "x_km": x, "y_km": y, "z_km": z,
                        "distance_earth_km": dist_e,
                        "distance_moon_km": 0,
                        "speed_km_h": speed,
                    })
                except (ValueError, IndexError):
                    pass
        logger.info("Horizons trajectory: %d points", len(points))
        return {"ref_frame": "J2000_EARTH", "point_count": len(points), "points": points}
    except Exception as e:
        logger.error("Horizons trajectory fetch failed: %s", e)
        return {"ref_frame": "J2000_EARTH", "point_count": 0, "points": []}


@cached(ttl_seconds=300)
def _fetch_path(window: str) -> dict[str, Any]:
    # Try DB first
    try:
        from app.db import execute_query, get_backend, table
        backend = get_backend()
        if backend in ("postgres", "databricks"):
            t = table('trajectory_history')
            sql = f"SELECT epoch_utc, x_km, y_km, z_km, distance_earth_km, distance_moon_km, speed_km_h FROM {t} ORDER BY epoch_utc"
            rows = execute_query(sql)
            if rows and len(rows) > 10:
                points = [
                    {
                        "epoch_utc": r.get("epoch_utc"),
                        "x_km": _parse_float(r.get("x_km")),
                        "y_km": _parse_float(r.get("y_km")),
                        "z_km": _parse_float(r.get("z_km")),
                        "distance_earth_km": _parse_float(r.get("distance_earth_km")),
                        "distance_moon_km": _parse_float(r.get("distance_moon_km")),
                        "speed_km_h": _parse_float(r.get("speed_km_h")),
                    }
                    for r in rows
                ]
                return {"ref_frame": "J2000_EARTH", "point_count": len(points), "points": points}
    except Exception as e:
        logger.warning("DB path query failed: %s", e)

    # Fallback to Horizons
    return _fetch_path_from_horizons()


@router.get("/path")
async def get_trajectory(window: str = Query(default="all")):
    return _fetch_path(window)
