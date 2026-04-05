# Databricks notebook source
# MAGIC %md
# MAGIC # Artemis II Tracker — Setup Tables
# MAGIC Creates the catalog, schemas, and all tables for the Artemis II tracking pipeline.
# MAGIC Fully idempotent: safe to re-run at any time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "artemis_tracker"

SCHEMAS = ["bronze", "silver", "gold", "serving"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"Schema {CATALOG}.{schema} ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables

# COMMAND ----------

# --- raw_horizons_vectors ---
spark.sql("""
CREATE TABLE IF NOT EXISTS artemis_tracker.bronze.raw_horizons_vectors (
    ingest_id       STRING,
    ingest_ts       TIMESTAMP,
    api_url         STRING,
    http_status     INT,
    response_hash   STRING,
    response_text   STRING,
    query_command   STRING,
    query_start     STRING,
    query_stop      STRING,
    query_step      STRING,
    api_version     STRING,
    latency_ms      LONG
) USING DELTA
""")
print("Table artemis_tracker.bronze.raw_horizons_vectors ready.")

# COMMAND ----------

# --- raw_arow_ephemeris ---
spark.sql("""
CREATE TABLE IF NOT EXISTS artemis_tracker.bronze.raw_arow_ephemeris (
    ingest_id       STRING,
    ingest_ts       TIMESTAMP,
    source_file     STRING,
    source_url      STRING,
    file_hash       STRING,
    epoch_utc       STRING,
    x_km            STRING,
    y_km            STRING,
    z_km            STRING,
    vx_km_s         STRING,
    vy_km_s         STRING,
    vz_km_s         STRING,
    raw_line        STRING
) USING DELTA
""")
print("Table artemis_tracker.bronze.raw_arow_ephemeris ready.")

# COMMAND ----------

# --- raw_nasa_media ---
spark.sql("""
CREATE TABLE IF NOT EXISTS artemis_tracker.bronze.raw_nasa_media (
    ingest_id       STRING,
    ingest_ts       TIMESTAMP,
    api_url         STRING,
    http_status     INT,
    response_json   STRING,
    total_hits      INT,
    latency_ms      LONG
) USING DELTA
""")
print("Table artemis_tracker.bronze.raw_nasa_media ready.")

# COMMAND ----------

# --- raw_mission_updates ---
spark.sql("""
CREATE TABLE IF NOT EXISTS artemis_tracker.bronze.raw_mission_updates (
    ingest_id       STRING,
    ingest_ts       TIMESTAMP,
    source_url      STRING,
    entry_id        STRING,
    title           STRING,
    published_at    STRING,
    content_text    STRING,
    author          STRING
) USING DELTA
""")
print("Table artemis_tracker.bronze.raw_mission_updates ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables

# COMMAND ----------

# --- telemetry_normalized ---
spark.sql("""
CREATE TABLE IF NOT EXISTS artemis_tracker.silver.telemetry_normalized (
    telemetry_id        STRING,
    epoch_utc           TIMESTAMP,
    mission_elapsed_s   DOUBLE,
    source              STRING,
    x_km                DOUBLE,
    y_km                DOUBLE,
    z_km                DOUBLE,
    vx_km_s             DOUBLE,
    vy_km_s             DOUBLE,
    vz_km_s             DOUBLE,
    distance_earth_km   DOUBLE,
    distance_moon_km    DOUBLE,
    speed_km_s          DOUBLE,
    speed_km_h          DOUBLE,
    lat_deg             DOUBLE,
    lon_deg             DOUBLE,
    altitude_km         DOUBLE,
    ingest_ts           TIMESTAMP,
    bronze_ingest_id    STRING
) USING DELTA
""")
print("Table artemis_tracker.silver.telemetry_normalized ready.")

# COMMAND ----------

# --- mission_events ---
spark.sql("""
CREATE TABLE IF NOT EXISTS artemis_tracker.silver.mission_events (
    event_id        STRING,
    event_ts        TIMESTAMP,
    event_type      STRING,
    event_name      STRING,
    description     STRING,
    source          STRING,
    phase           STRING,
    is_completed    BOOLEAN,
    planned_ts      TIMESTAMP,
    actual_ts       TIMESTAMP
) USING DELTA
""")
print("Table artemis_tracker.silver.mission_events ready.")

# COMMAND ----------

# --- media_catalog ---
spark.sql("""
CREATE TABLE IF NOT EXISTS artemis_tracker.silver.media_catalog (
    nasa_id         STRING,
    title           STRING,
    description     STRING,
    media_type      STRING,
    date_created    TIMESTAMP,
    thumbnail_url   STRING,
    full_url        STRING,
    center          STRING,
    first_seen_ts   TIMESTAMP
) USING DELTA
""")
print("Table artemis_tracker.silver.media_catalog ready.")

# COMMAND ----------

# --- data_quality_log ---
spark.sql("""
CREATE TABLE IF NOT EXISTS artemis_tracker.silver.data_quality_log (
    quality_id          STRING,
    ingest_ts           TIMESTAMP,
    source              STRING,
    ingest_id           STRING,
    record_count        INT,
    parse_error_count   INT,
    duplicate_count     INT,
    schema_hash         STRING,
    freshness_lag_s     DOUBLE,
    http_status         INT,
    latency_ms          LONG,
    is_healthy          BOOLEAN
) USING DELTA
""")
print("Table artemis_tracker.silver.data_quality_log ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Views

# COMMAND ----------

# --- current_status: latest telemetry row with computed fields ---
spark.sql("""
CREATE OR REPLACE VIEW artemis_tracker.gold.current_status AS
SELECT
    t.*,
    CASE
        WHEN t.distance_earth_km < 10000  THEN 'Near Earth'
        WHEN t.distance_moon_km  < 10000  THEN 'Near Moon'
        WHEN t.distance_earth_km < t.distance_moon_km THEN 'Outbound Transit'
        ELSE 'Return Transit'
    END AS flight_regime,
    TIMESTAMPDIFF(SECOND, TIMESTAMP '2026-04-01 22:35:00', t.epoch_utc) / 3600.0 AS mission_elapsed_hours,
    t.speed_km_s * 3600.0 AS current_speed_km_h
FROM artemis_tracker.silver.telemetry_normalized t
WHERE t.epoch_utc = (
    SELECT MAX(epoch_utc) FROM artemis_tracker.silver.telemetry_normalized
)
LIMIT 1
""")
print("View artemis_tracker.gold.current_status ready.")

# COMMAND ----------

# --- trajectory_history: all telemetry ordered by epoch ---
spark.sql("""
CREATE OR REPLACE VIEW artemis_tracker.gold.trajectory_history AS
SELECT *
FROM artemis_tracker.silver.telemetry_normalized
ORDER BY epoch_utc ASC
""")
print("View artemis_tracker.gold.trajectory_history ready.")

# COMMAND ----------

# --- milestones: mission events where event_type='milestone' ---
spark.sql("""
CREATE OR REPLACE VIEW artemis_tracker.gold.milestones AS
SELECT *
FROM artemis_tracker.silver.mission_events
WHERE event_type = 'milestone'
ORDER BY planned_ts ASC
""")
print("View artemis_tracker.gold.milestones ready.")

# COMMAND ----------

# --- diagnostics: aggregated data quality per source (last hour) ---
spark.sql("""
CREATE OR REPLACE VIEW artemis_tracker.gold.diagnostics AS
SELECT
    source,
    COUNT(*)                                AS checks_last_hour,
    SUM(record_count)                       AS total_records,
    SUM(parse_error_count)                  AS total_parse_errors,
    SUM(duplicate_count)                    AS total_duplicates,
    AVG(latency_ms)                         AS avg_latency_ms,
    MIN(ingest_ts)                          AS earliest_ingest,
    MAX(ingest_ts)                          AS latest_ingest,
    SUM(CASE WHEN is_healthy THEN 1 ELSE 0 END) AS healthy_count,
    SUM(CASE WHEN NOT is_healthy THEN 1 ELSE 0 END) AS unhealthy_count
FROM artemis_tracker.silver.data_quality_log
WHERE ingest_ts >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY source
""")
print("View artemis_tracker.gold.diagnostics ready.")

# COMMAND ----------

print("All tables and views created successfully.")
