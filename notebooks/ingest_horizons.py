# Databricks notebook source
# MAGIC %md
# MAGIC # Artemis II Tracker — Ingest JPL Horizons
# MAGIC Polls the JPL Horizons API for Orion (COMMAND='-1024') and Moon (COMMAND='301')
# MAGIC position vectors. Writes raw responses to bronze and logs data quality to silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("lookback_minutes", "30", "Lookback window (minutes)")
dbutils.widgets.text("step_size", "2 MINUTES", "Horizons STEP_SIZE")

LOOKBACK_MINUTES = int(dbutils.widgets.get("lookback_minutes"))
STEP_SIZE = dbutils.widgets.get("step_size")

# COMMAND ----------

import requests
import hashlib
import uuid
import time
import json
from datetime import datetime, timedelta, timezone

HORIZONS_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"

LAUNCH_TIME = datetime(2026, 4, 1, 22, 35, 0, tzinfo=timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def query_horizons(command: str, start_time: str, stop_time: str, step_size: str) -> dict:
    """Query JPL Horizons API and return response metadata + text."""
    params = {
        "format": "text",
        "COMMAND": command,
        "EPHEM_TYPE": "VECTORS",
        "CENTER": "500@399",       # Earth geocenter
        "CSV_FORMAT": "YES",
        "VEC_TABLE": "2",
        "MAKE_EPHEM": "YES",
        "OBJ_DATA": "NO",
        "START_TIME": start_time,
        "STOP_TIME": stop_time,
        "STEP_SIZE": step_size,
    }

    t0 = time.time()
    try:
        resp = requests.get(HORIZONS_URL, params=params, timeout=30)
        latency_ms = int((time.time() - t0) * 1000)
        return {
            "http_status": resp.status_code,
            "response_text": resp.text,
            "response_hash": hashlib.sha256(resp.text.encode()).hexdigest(),
            "latency_ms": latency_ms,
            "api_url": resp.url,
            "error": None,
        }
    except Exception as e:
        latency_ms = int((time.time() - t0) * 1000)
        return {
            "http_status": -1,
            "response_text": str(e),
            "response_hash": "",
            "latency_ms": latency_ms,
            "api_url": HORIZONS_URL,
            "error": str(e),
        }


def parse_horizons_csv(response_text: str) -> list:
    """
    Parse the CSV data between $$SOE and $$EOE markers.
    Returns list of dicts with: epoch_utc, x_km, y_km, z_km, vx_km_s, vy_km_s, vz_km_s
    """
    rows = []
    in_data = False
    for line in response_text.splitlines():
        stripped = line.strip()
        if stripped == "$$SOE":
            in_data = True
            continue
        if stripped == "$$EOE":
            in_data = False
            continue
        if in_data and stripped:
            # CSV format: JDTDB, Calendar Date (TDB), X, Y, Z, VX, VY, VZ, LT, RG, RR,
            parts = [p.strip() for p in stripped.split(",")]
            if len(parts) >= 8:
                try:
                    # parts[0] = JDTDB, parts[1] = Calendar Date (TDB), rest are vectors
                    calendar_date = parts[1].strip()
                    # Calendar Date format: "A.D. 2026-Apr-01 23:00:00.0000"
                    # Clean up for parsing
                    cal_clean = calendar_date.replace("A.D. ", "").strip()
                    rows.append({
                        "epoch_utc": cal_clean,
                        "jdtdb": parts[0].strip(),
                        "x_km": parts[2].strip(),
                        "y_km": parts[3].strip(),
                        "z_km": parts[4].strip(),
                        "vx_km_s": parts[5].strip(),
                        "vy_km_s": parts[6].strip(),
                        "vz_km_s": parts[7].strip(),
                    })
                except Exception:
                    pass  # skip malformed rows
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Query Window

# COMMAND ----------

now_utc = datetime.now(timezone.utc)
start_time = now_utc - timedelta(minutes=LOOKBACK_MINUTES)

# Format for Horizons: 'YYYY-MM-DD HH:MM'
fmt = "%Y-%m-%d %H:%M"
start_str = start_time.strftime(fmt)
stop_str = now_utc.strftime(fmt)

print(f"Query window: {start_str} to {stop_str}  (step: {STEP_SIZE})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Orion (COMMAND='-1024')

# COMMAND ----------

ingest_id_orion = str(uuid.uuid4())
ingest_ts = datetime.now(timezone.utc)

orion_result = query_horizons(
    command="'-1024'",
    start_time=start_str,
    stop_time=stop_str,
    step_size=STEP_SIZE,
)

print(f"Orion query: HTTP {orion_result['http_status']}, latency {orion_result['latency_ms']}ms, hash {orion_result['response_hash'][:16]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Moon (COMMAND='301') for distance computation

# COMMAND ----------

ingest_id_moon = str(uuid.uuid4())

moon_result = query_horizons(
    command="'301'",
    start_time=start_str,
    stop_time=stop_str,
    step_size=STEP_SIZE,
)

print(f"Moon query: HTTP {moon_result['http_status']}, latency {moon_result['latency_ms']}ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Raw Responses to Bronze

# COMMAND ----------

from pyspark.sql import Row

bronze_rows = [
    Row(
        ingest_id=ingest_id_orion,
        ingest_ts=ingest_ts,
        api_url=orion_result["api_url"],
        http_status=orion_result["http_status"],
        response_hash=orion_result["response_hash"],
        response_text=orion_result["response_text"],
        query_command="-1024",
        query_start=start_str,
        query_stop=stop_str,
        query_step=STEP_SIZE,
        api_version="1.0",
        latency_ms=int(orion_result["latency_ms"]),
    ),
    Row(
        ingest_id=ingest_id_moon,
        ingest_ts=ingest_ts,
        api_url=moon_result["api_url"],
        http_status=moon_result["http_status"],
        response_hash=moon_result["response_hash"],
        response_text=moon_result["response_text"],
        query_command="301",
        query_start=start_str,
        query_stop=stop_str,
        query_step=STEP_SIZE,
        api_version="1.0",
        latency_ms=int(moon_result["latency_ms"]),
    ),
]

df_bronze = spark.createDataFrame(bronze_rows)
df_bronze.write.mode("append").saveAsTable("artemis_tracker.bronze.raw_horizons_vectors")

print(f"Wrote {df_bronze.count()} rows to bronze.raw_horizons_vectors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Vectors and Log Quality

# COMMAND ----------

orion_vectors = parse_horizons_csv(orion_result["response_text"])
moon_vectors = parse_horizons_csv(moon_result["response_text"])

parse_errors_orion = 0
parse_errors_moon = 0

# If HTTP failed, everything is a parse error
if orion_result["http_status"] != 200:
    parse_errors_orion = -1  # signal full failure
if moon_result["http_status"] != 200:
    parse_errors_moon = -1

print(f"Parsed {len(orion_vectors)} Orion vectors, {len(moon_vectors)} Moon vectors")

# COMMAND ----------

# --- Log data quality for Orion ---
quality_rows = []

quality_rows.append(Row(
    quality_id=str(uuid.uuid4()),
    ingest_ts=ingest_ts,
    source="horizons_orion",
    ingest_id=ingest_id_orion,
    record_count=len(orion_vectors),
    parse_error_count=max(parse_errors_orion, 0),
    duplicate_count=0,
    schema_hash=hashlib.md5("orion_vectors_v1".encode()).hexdigest(),
    freshness_lag_s=0.0,
    http_status=orion_result["http_status"],
    latency_ms=int(orion_result["latency_ms"]),
    is_healthy=(orion_result["http_status"] == 200 and len(orion_vectors) > 0),
))

# --- Log data quality for Moon ---
quality_rows.append(Row(
    quality_id=str(uuid.uuid4()),
    ingest_ts=ingest_ts,
    source="horizons_moon",
    ingest_id=ingest_id_moon,
    record_count=len(moon_vectors),
    parse_error_count=max(parse_errors_moon, 0),
    duplicate_count=0,
    schema_hash=hashlib.md5("moon_vectors_v1".encode()).hexdigest(),
    freshness_lag_s=0.0,
    http_status=moon_result["http_status"],
    latency_ms=int(moon_result["latency_ms"]),
    is_healthy=(moon_result["http_status"] == 200 and len(moon_vectors) > 0),
))

df_quality = spark.createDataFrame(quality_rows)
df_quality.write.mode("append").saveAsTable("artemis_tracker.silver.data_quality_log")

print("Data quality logged to silver.data_quality_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("Ingest complete.")
print(f"  Orion ingest_id : {ingest_id_orion}")
print(f"  Moon  ingest_id : {ingest_id_moon}")
print(f"  Orion vectors   : {len(orion_vectors)}")
print(f"  Moon  vectors   : {len(moon_vectors)}")
print(f"  Orion HTTP      : {orion_result['http_status']}")
print(f"  Moon  HTTP      : {moon_result['http_status']}")
print("=" * 60)
