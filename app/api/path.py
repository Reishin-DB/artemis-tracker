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


def _get_flyby_moon_position(points: list[dict]) -> dict[str, float] | None:
    """Find the trajectory apex and fetch the Moon's position at that time."""
    if not points:
        return None
    # Find point with min distance to Moon (if available) or max distance from Earth
    apex = min(points, key=lambda p: p.get("distance_moon_km") or float("inf"))
    if not apex.get("epoch_utc") or (apex.get("distance_moon_km") or 0) == 0:
        apex = max(points, key=lambda p: p.get("distance_earth_km", 0))
    epoch = str(apex["epoch_utc"]).strip()
    # Query Moon position at apex time from Horizons
    try:
        from datetime import datetime, timedelta
        # Parse epoch — try common formats
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%b-%d %H:%M:%S.%f", "%Y-%b-%d %H:%M"):
            try:
                dt = datetime.strptime(epoch[:26], fmt)
                break
            except ValueError:
                continue
        else:
            logger.warning("Cannot parse apex epoch: %s", epoch)
            return None
        start = (dt - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M")
        stop = (dt + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M")
        moon_rows = _fetch_horizons_vectors("301", start, stop, "5 MINUTES")
        if moon_rows:
            r = moon_rows[-1]
            return {"x_km": r[1], "y_km": r[2], "z_km": r[3]}
    except Exception as e:
        logger.warning("Failed to get flyby Moon position: %s", e)
    return None


def _fetch_horizons_vectors(command: str, start: str, stop: str, step: str) -> list[tuple]:
    """Fetch position vectors from Horizons for a given body."""
    rows = []
    try:
        resp = requests.get("https://ssd.jpl.nasa.gov/api/horizons.api", params={
            "format": "text", "COMMAND": f"'{command}'", "EPHEM_TYPE": "'VECTORS'",
            "CENTER": "'500@399'", "START_TIME": f"'{start}'",
            "STOP_TIME": f"'{stop}'", "STEP_SIZE": f"'{step}'",
            "REF_PLANE": "'FRAME'", "VEC_TABLE": "'2'",
            "MAKE_EPHEM": "'YES'", "OBJ_DATA": "'NO'", "CSV_FORMAT": "'YES'"
        }, timeout=45)
        in_data = False
        for line in resp.text.split("\n"):
            line = line.strip()
            if "$$SOE" in line: in_data = True; continue
            if "$$EOE" in line: break
            if not in_data or not line: continue
            parts = [p.strip().rstrip(",") for p in line.split(",")]
            if len(parts) >= 8:
                try:
                    rows.append((
                        parts[1].strip().replace("A.D. ", ""),
                        float(parts[2]), float(parts[3]), float(parts[4]),
                        float(parts[5]), float(parts[6]), float(parts[7]),
                    ))
                except (ValueError, IndexError):
                    pass
    except Exception as e:
        logger.warning("Horizons fetch for %s failed: %s", command, e)
    return rows


def _fetch_path_from_horizons() -> dict[str, Any]:
    """Fetch full trajectory from Horizons: launch to splashdown, 15-min steps."""
    start, stop, step = "2026-04-02 02:00", "2026-04-10 18:00", "15 MINUTES"

    orion_rows = _fetch_horizons_vectors("-1024", start, stop, step)
    moon_rows = _fetch_horizons_vectors("301", start, stop, step)

    # Build moon lookup by index (same timesteps)
    moon_by_idx = {i: r for i, r in enumerate(moon_rows)}

    points = []
    flyby_moon_pos = None
    min_moon_dist = float("inf")

    for i, (epoch, x, y, z, vx, vy, vz) in enumerate(orion_rows):
        dist_e = math.sqrt(x**2 + y**2 + z**2)
        speed = math.sqrt(vx**2 + vy**2 + vz**2) * 3600
        dist_m = 0.0

        moon = moon_by_idx.get(i)
        if moon:
            mx, my, mz = moon[1], moon[2], moon[3]
            dist_m = math.sqrt((x - mx)**2 + (y - my)**2 + (z - mz)**2)
            if dist_m < min_moon_dist:
                min_moon_dist = dist_m
                flyby_moon_pos = {"x_km": mx, "y_km": my, "z_km": mz}

        points.append({
            "epoch_utc": epoch,
            "x_km": x, "y_km": y, "z_km": z,
            "distance_earth_km": dist_e,
            "distance_moon_km": dist_m,
            "speed_km_h": speed,
        })

    logger.info("Horizons trajectory: %d orion points, %d moon points, flyby dist %.0f km",
                len(points), len(moon_rows), min_moon_dist)

    return {
        "ref_frame": "J2000_EARTH",
        "point_count": len(points),
        "points": points,
        "flyby_moon_position": flyby_moon_pos,
    }


@cached(ttl_seconds=300)
def _fetch_path(window: str) -> dict[str, Any]:
    # Try DB first
    try:
        from datetime import datetime, timedelta
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

                # Extend with predicted Horizons trajectory if DB doesn't
                # cover the full mission (through Apr 10 18:00 splashdown).
                # This ensures the return-to-Earth arc is always visible.
                MISSION_END = "2026-04-10 18:00"
                last_epoch = str(points[-1].get("epoch_utc", ""))
                try:
                    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%b-%d %H:%M:%S.%f", "%Y-%b-%d %H:%M"):
                        try:
                            last_dt = datetime.strptime(last_epoch[:26], fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        last_dt = None

                    mission_end_dt = datetime(2026, 4, 10, 18, 0)
                    if last_dt and last_dt < mission_end_dt - timedelta(hours=1):
                        future_start = (last_dt + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M")
                        future_rows = _fetch_horizons_vectors("-1024", future_start, MISSION_END, "15 MINUTES")
                        moon_future = _fetch_horizons_vectors("301", future_start, MISSION_END, "15 MINUTES")
                        moon_idx = {i: r for i, r in enumerate(moon_future)}
                        for i, (epoch, x, y, z, vx, vy, vz) in enumerate(future_rows):
                            dist_e = math.sqrt(x**2 + y**2 + z**2)
                            speed = math.sqrt(vx**2 + vy**2 + vz**2) * 3600
                            dist_m = 0.0
                            moon = moon_idx.get(i)
                            if moon:
                                mx, my, mz = moon[1], moon[2], moon[3]
                                dist_m = math.sqrt((x - mx)**2 + (y - my)**2 + (z - mz)**2)
                            points.append({
                                "epoch_utc": epoch,
                                "x_km": x, "y_km": y, "z_km": z,
                                "distance_earth_km": dist_e,
                                "distance_moon_km": dist_m,
                                "speed_km_h": speed,
                            })
                        logger.info("Extended trajectory with %d predicted points through splashdown", len(future_rows))
                except Exception as ext_err:
                    logger.warning("Failed to extend trajectory with Horizons: %s", ext_err)

                flyby_moon_pos = _get_flyby_moon_position(points)
                return {"ref_frame": "J2000_EARTH", "point_count": len(points), "points": points, "flyby_moon_position": flyby_moon_pos}
    except Exception as e:
        logger.warning("DB path query failed: %s", e)

    # Fallback to Horizons (already fetches full mission window)
    return _fetch_path_from_horizons()


@router.get("/path")
async def get_trajectory(window: str = Query(default="all")):
    return _fetch_path(window)
