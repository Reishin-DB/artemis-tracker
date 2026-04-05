# Databricks notebook source
# MAGIC %md
# MAGIC # Artemis II Tracker — Transform Silver
# MAGIC Transforms bronze Horizons data into silver.telemetry_normalized.
# MAGIC Parses raw CSV vectors, computes derived fields, deduplicates, and MERGEs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("lookback_hours", "2", "Process bronze data from last N hours")

LOOKBACK_HOURS = int(dbutils.widgets.get("lookback_hours"))

# COMMAND ----------

import math
import hashlib
import uuid
from datetime import datetime, timezone

LAUNCH_TIME_UTC = datetime(2026, 4, 1, 22, 35, 0, tzinfo=timezone.utc)
LAUNCH_EPOCH_S = LAUNCH_TIME_UTC.timestamp()
EARTH_RADIUS_KM = 6371.0

# Month abbreviation map for Horizons calendar date format
MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Recent Bronze Data

# COMMAND ----------

df_bronze = spark.sql(f"""
    SELECT ingest_id, ingest_ts, query_command, response_text
    FROM artemis_tracker.bronze.raw_horizons_vectors
    WHERE ingest_ts >= current_timestamp() - INTERVAL {LOOKBACK_HOURS} HOURS
      AND http_status = 200
    ORDER BY ingest_ts DESC
""")

bronze_count = df_bronze.count()
print(f"Found {bronze_count} bronze rows from the last {LOOKBACK_HOURS} hours")

if bronze_count == 0:
    dbutils.notebook.exit("No new bronze data to transform.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Horizons CSV Vectors

# COMMAND ----------

def parse_horizons_csv(response_text: str) -> list:
    """Parse CSV rows between $$SOE and $$EOE markers."""
    rows = []
    in_data = False
    for line in response_text.splitlines():
        stripped = line.strip()
        if stripped == "$$SOE":
            in_data = True
            continue
        if stripped == "$$EOE":
            break
        if in_data and stripped:
            parts = [p.strip() for p in stripped.split(",")]
            if len(parts) >= 8:
                rows.append({
                    "jdtdb": parts[0],
                    "calendar_date": parts[1].strip(),
                    "x_km": parts[2],
                    "y_km": parts[3],
                    "z_km": parts[4],
                    "vx_km_s": parts[5],
                    "vy_km_s": parts[6],
                    "vz_km_s": parts[7],
                })
    return rows


def parse_calendar_date(cal_str: str) -> datetime:
    """
    Parse Horizons calendar date like '2026-Apr-01 23:00:00.0000' (with or without 'A.D. ' prefix)
    into a Python datetime (UTC).
    """
    clean = cal_str.replace("A.D. ", "").strip()
    # Format: 2026-Apr-01 23:00:00.0000
    parts = clean.split()
    date_part = parts[0]  # 2026-Apr-01
    time_part = parts[1] if len(parts) > 1 else "00:00:00.0000"

    yr, mon_abbr, day = date_part.split("-")
    mon = MONTH_MAP.get(mon_abbr, "01")

    # Truncate fractional seconds to 6 digits for strptime
    time_clean = time_part[:15]  # "HH:MM:SS.ffffff"

    dt_str = f"{yr}-{mon}-{day} {time_clean}"
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

    return dt.replace(tzinfo=timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Orion + Moon Vector Lookup

# COMMAND ----------

bronze_rows = df_bronze.collect()

# Separate Orion and Moon responses
orion_vectors_all = []  # (ingest_id, parsed rows)
moon_vectors_all = []

for row in bronze_rows:
    parsed = parse_horizons_csv(row["response_text"])
    if row["query_command"] == "-1024":
        orion_vectors_all.append((row["ingest_id"], row["ingest_ts"], parsed))
    elif row["query_command"] == "301":
        moon_vectors_all.append((row["ingest_id"], row["ingest_ts"], parsed))

print(f"Orion batches: {len(orion_vectors_all)}, Moon batches: {len(moon_vectors_all)}")

# COMMAND ----------

# Build Moon position lookup keyed by JDTDB for distance computation
moon_lookup = {}  # jdtdb -> (mx, my, mz)
for _, _, moon_rows in moon_vectors_all:
    for mv in moon_rows:
        try:
            moon_lookup[mv["jdtdb"]] = (
                float(mv["x_km"]),
                float(mv["y_km"]),
                float(mv["z_km"]),
            )
        except (ValueError, KeyError):
            pass

print(f"Moon position lookup: {len(moon_lookup)} epochs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Derived Fields and Build Silver Rows

# COMMAND ----------

from pyspark.sql import Row

silver_rows = []
parse_errors = 0

for ingest_id, ingest_ts, orion_rows in orion_vectors_all:
    for ov in orion_rows:
        try:
            x = float(ov["x_km"])
            y = float(ov["y_km"])
            z = float(ov["z_km"])
            vx = float(ov["vx_km_s"])
            vy = float(ov["vy_km_s"])
            vz = float(ov["vz_km_s"])

            epoch_dt = parse_calendar_date(ov["calendar_date"])
            epoch_s = epoch_dt.timestamp()
            mission_elapsed_s = epoch_s - LAUNCH_EPOCH_S

            # Derived spatial fields
            distance_earth_km = math.sqrt(x**2 + y**2 + z**2)
            speed_km_s = math.sqrt(vx**2 + vy**2 + vz**2)
            speed_km_h = speed_km_s * 3600.0
            altitude_km = distance_earth_km - EARTH_RADIUS_KM

            # Lat/Lon (geocentric)
            if distance_earth_km > 0:
                lat_deg = math.degrees(math.asin(z / distance_earth_km))
                lon_deg = math.degrees(math.atan2(y, x))
            else:
                lat_deg = 0.0
                lon_deg = 0.0

            # Distance to Moon
            distance_moon_km = None
            moon_pos = moon_lookup.get(ov["jdtdb"])
            if moon_pos:
                mx, my, mz = moon_pos
                dx = x - mx
                dy = y - my
                dz = z - mz
                distance_moon_km = math.sqrt(dx**2 + dy**2 + dz**2)

            # Telemetry ID: deterministic hash of epoch + source
            tid_raw = f"{epoch_dt.isoformat()}|horizons"
            telemetry_id = hashlib.sha256(tid_raw.encode()).hexdigest()[:32]

            silver_rows.append(Row(
                telemetry_id=telemetry_id,
                epoch_utc=epoch_dt,
                mission_elapsed_s=mission_elapsed_s,
                source="horizons",
                x_km=x,
                y_km=y,
                z_km=z,
                vx_km_s=vx,
                vy_km_s=vy,
                vz_km_s=vz,
                distance_earth_km=distance_earth_km,
                distance_moon_km=distance_moon_km,
                speed_km_s=speed_km_s,
                speed_km_h=speed_km_h,
                lat_deg=lat_deg,
                lon_deg=lon_deg,
                altitude_km=altitude_km,
                ingest_ts=ingest_ts,
                bronze_ingest_id=ingest_id,
            ))
        except Exception as e:
            parse_errors += 1
            print(f"Parse error: {e} | row: {ov}")

print(f"Built {len(silver_rows)} silver rows, {parse_errors} parse errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into Silver Telemetry

# COMMAND ----------

if silver_rows:
    df_silver = spark.createDataFrame(silver_rows)
    df_silver.createOrReplaceTempView("staging_telemetry")

    merge_sql = """
    MERGE INTO artemis_tracker.silver.telemetry_normalized AS target
    USING staging_telemetry AS source
    ON target.telemetry_id = source.telemetry_id
    WHEN MATCHED AND source.ingest_ts > target.ingest_ts THEN UPDATE SET
        target.epoch_utc          = source.epoch_utc,
        target.mission_elapsed_s  = source.mission_elapsed_s,
        target.source             = source.source,
        target.x_km               = source.x_km,
        target.y_km               = source.y_km,
        target.z_km               = source.z_km,
        target.vx_km_s            = source.vx_km_s,
        target.vy_km_s            = source.vy_km_s,
        target.vz_km_s            = source.vz_km_s,
        target.distance_earth_km  = source.distance_earth_km,
        target.distance_moon_km   = source.distance_moon_km,
        target.speed_km_s         = source.speed_km_s,
        target.speed_km_h         = source.speed_km_h,
        target.lat_deg            = source.lat_deg,
        target.lon_deg            = source.lon_deg,
        target.altitude_km        = source.altitude_km,
        target.ingest_ts          = source.ingest_ts,
        target.bronze_ingest_id   = source.bronze_ingest_id
    WHEN NOT MATCHED THEN INSERT *
    """

    result = spark.sql(merge_sql)
    display(result)
    print("MERGE complete.")
else:
    print("No silver rows to merge.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Data Quality

# COMMAND ----------

from pyspark.sql import Row as QRow

quality_row = QRow(
    quality_id=str(uuid.uuid4()),
    ingest_ts=datetime.now(timezone.utc),
    source="transform_silver",
    ingest_id="batch",
    record_count=len(silver_rows),
    parse_error_count=parse_errors,
    duplicate_count=0,  # MERGE handles dedup
    schema_hash=hashlib.md5("telemetry_normalized_v1".encode()).hexdigest(),
    freshness_lag_s=0.0,
    http_status=200,
    latency_ms=0,
    is_healthy=(len(silver_rows) > 0 and parse_errors == 0),
)

df_quality = spark.createDataFrame([quality_row])
df_quality.write.mode("append").saveAsTable("artemis_tracker.silver.data_quality_log")

print("Quality metrics logged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_telemetry = spark.sql("SELECT COUNT(*) AS cnt FROM artemis_tracker.silver.telemetry_normalized").collect()[0]["cnt"]

print("=" * 60)
print("Transform complete.")
print(f"  Bronze batches processed : {len(orion_vectors_all)}")
print(f"  Silver rows merged       : {len(silver_rows)}")
print(f"  Parse errors             : {parse_errors}")
print(f"  Total telemetry records  : {total_telemetry}")
print("=" * 60)
