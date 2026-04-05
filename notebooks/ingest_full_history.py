# Databricks notebook: ingest_full_history.py
# Purpose: One-time backfill of full Artemis II trajectory from launch to now
# Run this ONCE to populate historical data, then rely on ingest_horizons.py for incremental updates

import requests
import hashlib
import uuid
import math
from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType, LongType, IntegerType
)

# --- Config ---
HORIZONS_API = "https://ssd.jpl.nasa.gov/api/horizons.api"
LAUNCH_EPOCH_UTC = datetime(2026, 4, 1, 22, 35, 0, tzinfo=timezone.utc)
# Ephemeris starts after ICPS separation (~3.5h post-launch)
EPHEM_START = "2026-04-02 02:00"
EARTH_RADIUS_KM = 6371.0

# --- Fetch Orion full trajectory ---
print("Fetching full Orion trajectory from Horizons...")
now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

orion_params = {
    "format": "text",
    "COMMAND": "'-1024'",
    "EPHEM_TYPE": "'VECTORS'",
    "CENTER": "'500@399'",
    "START_TIME": f"'{EPHEM_START}'",
    "STOP_TIME": f"'{now_str}'",
    "STEP_SIZE": "'4 MINUTES'",
    "REF_PLANE": "'FRAME'",
    "VEC_TABLE": "'2'",
    "MAKE_EPHEM": "'YES'",
    "OBJ_DATA": "'NO'",
    "CSV_FORMAT": "'YES'"
}

t0 = datetime.now(timezone.utc)
orion_resp = requests.get(HORIZONS_API, params=orion_params, timeout=120)
latency_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
print(f"Orion response: {orion_resp.status_code}, {len(orion_resp.text)} bytes, {latency_ms}ms")

# --- Fetch Moon positions for same window ---
print("Fetching Moon positions from Horizons...")
moon_params = {
    "format": "text",
    "COMMAND": "'301'",
    "EPHEM_TYPE": "'VECTORS'",
    "CENTER": "'500@399'",
    "START_TIME": f"'{EPHEM_START}'",
    "STOP_TIME": f"'{now_str}'",
    "STEP_SIZE": "'30 MINUTES'",
    "REF_PLANE": "'FRAME'",
    "VEC_TABLE": "'2'",
    "MAKE_EPHEM": "'YES'",
    "OBJ_DATA": "'NO'",
    "CSV_FORMAT": "'YES'"
}

moon_resp = requests.get(HORIZONS_API, params=moon_params, timeout=120)
print(f"Moon response: {moon_resp.status_code}, {len(moon_resp.text)} bytes")


def parse_horizons_csv(text):
    """Parse Horizons CSV output between $$SOE and $$EOE markers."""
    vectors = []
    in_data = False
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("$$SOE"):
            in_data = True
            continue
        if line.startswith("$$EOE"):
            break
        if not in_data or not line:
            continue
        parts = [p.strip().rstrip(",") for p in line.split(",")]
        if len(parts) >= 8:
            try:
                vectors.append({
                    "jd": float(parts[0]),
                    "cal": parts[1].strip(),
                    "x": float(parts[2]),
                    "y": float(parts[3]),
                    "z": float(parts[4]),
                    "vx": float(parts[5]),
                    "vy": float(parts[6]),
                    "vz": float(parts[7]),
                })
            except (ValueError, IndexError):
                continue
    return vectors


def cal_to_datetime(cal_str):
    """Convert Horizons calendar string to datetime."""
    # Format: "A.D. 2026-Apr-04 12:00:00.0000"
    cal_str = cal_str.replace("A.D. ", "").strip()
    try:
        return datetime.strptime(cal_str[:19], "%Y-%b-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.strptime(cal_str[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None


# Parse Orion vectors
orion_vectors = parse_horizons_csv(orion_resp.text)
print(f"Parsed {len(orion_vectors)} Orion vectors")

# Parse Moon vectors
moon_vectors = parse_horizons_csv(moon_resp.text)
print(f"Parsed {len(moon_vectors)} Moon vectors")

# Build Moon lookup by JD (nearest 30-min)
moon_by_jd = {}
for mv in moon_vectors:
    moon_by_jd[round(mv["jd"] * 48) / 48] = mv  # Round to nearest 30 min


def get_nearest_moon(jd):
    """Get nearest Moon position for a given Julian Date."""
    key = round(jd * 48) / 48
    if key in moon_by_jd:
        return moon_by_jd[key]
    # Linear search for nearest
    best = None
    best_dist = float("inf")
    for mjd, mv in moon_by_jd.items():
        d = abs(mjd - jd)
        if d < best_dist:
            best_dist = d
            best = mv
    return best


# --- Build silver telemetry rows ---
print("Building telemetry records...")
ingest_id = str(uuid.uuid4())
ingest_ts = datetime.now(timezone.utc)
rows = []

for v in orion_vectors:
    epoch = cal_to_datetime(v["cal"])
    if epoch is None:
        continue

    x, y, z = v["x"], v["y"], v["z"]
    vx, vy, vz = v["vx"], v["vy"], v["vz"]

    dist_earth = math.sqrt(x**2 + y**2 + z**2)
    speed_km_s = math.sqrt(vx**2 + vy**2 + vz**2)
    speed_km_h = speed_km_s * 3600.0
    altitude = dist_earth - EARTH_RADIUS_KM
    lat_deg = math.degrees(math.asin(z / dist_earth)) if dist_earth > 0 else 0.0
    lon_deg = math.degrees(math.atan2(y, x))
    mission_elapsed = (epoch - LAUNCH_EPOCH_UTC).total_seconds()

    # Moon distance
    moon = get_nearest_moon(v["jd"])
    if moon:
        dist_moon = math.sqrt(
            (x - moon["x"])**2 + (y - moon["y"])**2 + (z - moon["z"])**2
        )
    else:
        dist_moon = None

    tid = hashlib.sha256(f"{epoch.isoformat()}|horizons_vectors".encode()).hexdigest()[:32]

    rows.append(Row(
        telemetry_id=tid,
        epoch_utc=epoch,
        mission_elapsed_s=float(mission_elapsed),
        source="horizons_vectors",
        x_km=float(x),
        y_km=float(y),
        z_km=float(z),
        vx_km_s=float(vx),
        vy_km_s=float(vy),
        vz_km_s=float(vz),
        distance_earth_km=float(dist_earth),
        distance_moon_km=float(dist_moon) if dist_moon else None,
        speed_km_s=float(speed_km_s),
        speed_km_h=float(speed_km_h),
        lat_deg=float(lat_deg),
        lon_deg=float(lon_deg),
        altitude_km=float(altitude),
        ingest_ts=ingest_ts,
        bronze_ingest_id=ingest_id,
    ))

print(f"Built {len(rows)} telemetry records")

if rows:
    schema = StructType([
        StructField("telemetry_id", StringType()),
        StructField("epoch_utc", TimestampType()),
        StructField("mission_elapsed_s", DoubleType()),
        StructField("source", StringType()),
        StructField("x_km", DoubleType()),
        StructField("y_km", DoubleType()),
        StructField("z_km", DoubleType()),
        StructField("vx_km_s", DoubleType()),
        StructField("vy_km_s", DoubleType()),
        StructField("vz_km_s", DoubleType()),
        StructField("distance_earth_km", DoubleType()),
        StructField("distance_moon_km", DoubleType()),
        StructField("speed_km_s", DoubleType()),
        StructField("speed_km_h", DoubleType()),
        StructField("lat_deg", DoubleType()),
        StructField("lon_deg", DoubleType()),
        StructField("altitude_km", DoubleType()),
        StructField("ingest_ts", TimestampType()),
        StructField("bronze_ingest_id", StringType()),
    ])

    df = spark.createDataFrame(rows, schema=schema)

    # Write to silver telemetry - use MERGE to avoid duplicates
    df.createOrReplaceTempView("new_telemetry")

    spark.sql("""
        MERGE INTO artemis_tracker.silver.telemetry_normalized AS target
        USING new_telemetry AS source
        ON target.telemetry_id = source.telemetry_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    final_count = spark.sql("SELECT COUNT(*) as cnt FROM artemis_tracker.silver.telemetry_normalized").collect()[0]["cnt"]
    print(f"Silver telemetry now has {final_count} records")

    # Also save raw response to bronze
    bronze_row = Row(
        ingest_id=ingest_id,
        ingest_ts=ingest_ts,
        api_url=orion_resp.url[:500],
        http_status=orion_resp.status_code,
        response_hash=hashlib.sha256(orion_resp.text.encode()).hexdigest(),
        response_text=orion_resp.text[:500000],
        query_command="-1024",
        query_start=EPHEM_START,
        query_stop=now_str,
        query_step="4 MINUTES",
        api_version="1.2",
        latency_ms=long(latency_ms),
    )
    spark.createDataFrame([bronze_row]).write.mode("append").saveAsTable(
        "artemis_tracker.bronze.raw_horizons_vectors"
    )
    print("Bronze raw response saved")

print("Full history ingestion complete!")
print(f"Coverage: {orion_vectors[0]['cal'] if orion_vectors else 'N/A'} to {orion_vectors[-1]['cal'] if orion_vectors else 'N/A'}")
print(f"First distance: {rows[0].distance_earth_km:.0f} km" if rows else "")
print(f"Last distance: {rows[-1].distance_earth_km:.0f} km" if rows else "")
