# Databricks notebook source
# MAGIC %md
# MAGIC # Artemis II Tracker — Sync to Lakebase
# MAGIC Reads from Unity Catalog gold/silver views and writes to Lakebase (PostgreSQL)
# MAGIC serving tables. No mocked data — only real pipeline data.
# MAGIC
# MAGIC **Schedule:** Every 5 minutes via Databricks Workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import os
import math
from datetime import datetime, timezone

# Lakebase connection — set via job parameters or notebook widgets
dbutils.widgets.text("pg_host", "", "Lakebase host")
dbutils.widgets.text("pg_database", "artemis_app", "Lakebase database name")
dbutils.widgets.text("pg_project", "artemis-tracker-db", "Lakebase project ID")

PG_HOST = dbutils.widgets.get("pg_host")
PG_DATABASE = dbutils.widgets.get("pg_database")
PG_PROJECT = dbutils.widgets.get("pg_project")

LAUNCH_TIME_UTC = datetime(2026, 4, 1, 22, 35, 0, tzinfo=timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Lakebase

# COMMAND ----------

def get_lakebase_connection():
    """Get a psycopg2 connection to Lakebase using Databricks OAuth."""
    import subprocess
    import json
    import psycopg2

    if PG_HOST:
        host = PG_HOST
    else:
        # Auto-discover host from project
        result = subprocess.run([
            "databricks", "postgres", "list-endpoints",
            f"projects/{PG_PROJECT}/branches/production",
            "--output", "json"
        ], capture_output=True, text=True)
        endpoints = json.loads(result.stdout)
        host = endpoints[0]["status"]["hosts"]["host"]

    # Generate OAuth credential
    result = subprocess.run([
        "databricks", "postgres", "generate-database-credential",
        f"projects/{PG_PROJECT}/branches/production/endpoints/primary",
        "--output", "json"
    ], capture_output=True, text=True)
    cred = json.loads(result.stdout)

    # Get current user email
    result = subprocess.run([
        "databricks", "current-user", "me", "--output", "json"
    ], capture_output=True, text=True)
    email = json.loads(result.stdout)["userName"]

    conn = psycopg2.connect(
        host=host,
        port=5432,
        dbname=PG_DATABASE,
        user=email,
        password=cred["token"],
        sslmode="require",
        connect_timeout=15,
    )
    conn.autocommit = True
    return conn

conn = get_lakebase_connection()
print(f"Connected to Lakebase: {PG_DATABASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Sync Current Status
# MAGIC Latest telemetry row from silver → current_status single-row table.

# COMMAND ----------

df_current = spark.sql("""
    SELECT *
    FROM artemis_tracker.silver.telemetry_normalized
    ORDER BY epoch_utc DESC
    LIMIT 1
""")

if df_current.count() > 0:
    row = df_current.collect()[0]
    now = datetime.now(timezone.utc)
    elapsed = (now - LAUNCH_TIME_UTC).total_seconds()

    x = float(row["x_km"])
    y = float(row["y_km"])
    z = float(row["z_km"])
    vx = float(row["vx_km_s"])
    vy = float(row["vy_km_s"])
    vz = float(row["vz_km_s"])
    dist_e = float(row["distance_earth_km"])
    dist_m = float(row["distance_moon_km"]) if row["distance_moon_km"] else 0.0
    speed_km_h = float(row["speed_km_h"])

    # Determine phase from distances
    if dist_e < 10000:
        phase = "near_earth"
    elif dist_m < 10000:
        phase = "lunar_flyby"
    elif dist_e < dist_m:
        phase = "transit_out"
    else:
        phase = "transit_return"

    # Find last completed milestone
    df_last_ms = spark.sql("""
        SELECT event_name FROM artemis_tracker.silver.mission_events
        WHERE is_completed = true AND event_type = 'milestone'
        ORDER BY planned_ts DESC LIMIT 1
    """)
    last_milestone = ""
    if df_last_ms.count() > 0:
        last_milestone = df_last_ms.collect()[0]["event_name"]

    d = int(elapsed // 86400)
    h = int((elapsed % 86400) // 3600)
    m = int((elapsed % 3600) // 60)

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO current_status (
            id, last_update_utc, mission_elapsed_s, mission_elapsed_display,
            current_phase, last_milestone,
            distance_earth_km, distance_earth_miles,
            distance_moon_km, distance_moon_miles,
            speed_km_h, speed_mph,
            x_km, y_km, z_km,
            vx_km_s, vy_km_s, vz_km_s,
            lat_deg, lon_deg, altitude_km,
            data_source, staleness_seconds, stale, updated_at
        ) VALUES (
            1, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, 0, false, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            last_update_utc = EXCLUDED.last_update_utc,
            mission_elapsed_s = EXCLUDED.mission_elapsed_s,
            mission_elapsed_display = EXCLUDED.mission_elapsed_display,
            current_phase = EXCLUDED.current_phase,
            last_milestone = EXCLUDED.last_milestone,
            distance_earth_km = EXCLUDED.distance_earth_km,
            distance_earth_miles = EXCLUDED.distance_earth_miles,
            distance_moon_km = EXCLUDED.distance_moon_km,
            distance_moon_miles = EXCLUDED.distance_moon_miles,
            speed_km_h = EXCLUDED.speed_km_h,
            speed_mph = EXCLUDED.speed_mph,
            x_km = EXCLUDED.x_km, y_km = EXCLUDED.y_km, z_km = EXCLUDED.z_km,
            vx_km_s = EXCLUDED.vx_km_s, vy_km_s = EXCLUDED.vy_km_s, vz_km_s = EXCLUDED.vz_km_s,
            lat_deg = EXCLUDED.lat_deg, lon_deg = EXCLUDED.lon_deg, altitude_km = EXCLUDED.altitude_km,
            data_source = EXCLUDED.data_source,
            staleness_seconds = 0, stale = false, updated_at = NOW()
    """, (
        row["epoch_utc"], elapsed, f"{d}d {h}h {m}m",
        phase, last_milestone,
        dist_e, dist_e * 0.621371,
        dist_m, dist_m * 0.621371,
        speed_km_h, speed_km_h * 0.621371,
        x, y, z, vx, vy, vz,
        float(row["lat_deg"]) if row["lat_deg"] else 0.0,
        float(row["lon_deg"]) if row["lon_deg"] else 0.0,
        float(row["altitude_km"]) if row["altitude_km"] else dist_e - 6371.0,
        row["source"],
    ))
    print(f"current_status synced — phase={phase}, dist_earth={dist_e:.0f} km")
else:
    print("No telemetry data to sync for current_status.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sync Trajectory History
# MAGIC Incremental: only insert new epochs not already in Lakebase.

# COMMAND ----------

# Get latest epoch already in Lakebase
cur = conn.cursor()
cur.execute("SELECT MAX(epoch_utc) FROM trajectory_history")
max_epoch = cur.fetchone()[0]

if max_epoch:
    print(f"Lakebase trajectory latest epoch: {max_epoch}")
    df_new = spark.sql(f"""
        SELECT epoch_utc, mission_elapsed_s, x_km, y_km, z_km,
               distance_earth_km, distance_moon_km, speed_km_h, source
        FROM artemis_tracker.silver.telemetry_normalized
        WHERE epoch_utc > TIMESTAMP '{max_epoch}'
        ORDER BY epoch_utc
    """)
else:
    print("Lakebase trajectory is empty — full backfill")
    df_new = spark.sql("""
        SELECT epoch_utc, mission_elapsed_s, x_km, y_km, z_km,
               distance_earth_km, distance_moon_km, speed_km_h, source
        FROM artemis_tracker.silver.telemetry_normalized
        ORDER BY epoch_utc
    """)

new_count = df_new.count()
print(f"New trajectory points to sync: {new_count}")

if new_count > 0:
    rows = df_new.collect()
    cur = conn.cursor()
    insert_sql = """
        INSERT INTO trajectory_history
            (epoch_utc, mission_elapsed_s, x_km, y_km, z_km,
             distance_earth_km, distance_moon_km, speed_km_h, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (epoch_utc) DO NOTHING
    """
    batch = []
    for r in rows:
        batch.append((
            r["epoch_utc"],
            float(r["mission_elapsed_s"]) if r["mission_elapsed_s"] else 0.0,
            float(r["x_km"]),
            float(r["y_km"]),
            float(r["z_km"]),
            float(r["distance_earth_km"]) if r["distance_earth_km"] else 0.0,
            float(r["distance_moon_km"]) if r["distance_moon_km"] else 0.0,
            float(r["speed_km_h"]) if r["speed_km_h"] else 0.0,
            r["source"],
        ))
    from psycopg2.extras import execute_batch
    execute_batch(cur, insert_sql, batch, page_size=500)
    print(f"Inserted {len(batch)} trajectory points into Lakebase.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sync Milestones
# MAGIC Upsert mission milestones — status may change as events complete.

# COMMAND ----------

df_milestones = spark.sql("""
    SELECT event_id, event_name, description, phase,
           planned_ts, actual_ts, is_completed,
           CASE
               WHEN is_completed = true THEN 'completed'
               WHEN planned_ts <= current_timestamp() AND is_completed = false THEN 'in_progress'
               ELSE 'upcoming'
           END AS status
    FROM artemis_tracker.silver.mission_events
    WHERE event_type = 'milestone'
    ORDER BY planned_ts
""")

ms_count = df_milestones.count()
print(f"Milestones to sync: {ms_count}")

if ms_count > 0:
    cur = conn.cursor()
    for r in df_milestones.collect():
        cur.execute("""
            INSERT INTO milestones (event_id, event_name, description, phase, planned_ts, actual_ts, is_completed, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_name) DO UPDATE SET
                description = EXCLUDED.description,
                phase = EXCLUDED.phase,
                planned_ts = EXCLUDED.planned_ts,
                actual_ts = EXCLUDED.actual_ts,
                is_completed = EXCLUDED.is_completed,
                status = EXCLUDED.status
        """, (
            r["event_id"], r["event_name"], r["description"], r["phase"],
            r["planned_ts"], r["actual_ts"], r["is_completed"],
            r["status"],
        ))
    print(f"Milestones synced ({ms_count} rows).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sync Media Catalog
# MAGIC Upsert NASA media items from silver.

# COMMAND ----------

df_media = spark.sql("""
    SELECT nasa_id, title, description, media_type,
           date_created, thumbnail_url, full_url, center
    FROM artemis_tracker.silver.media_catalog
    ORDER BY date_created DESC
    LIMIT 50
""")

media_count = df_media.count()
print(f"Media items to sync: {media_count}")

if media_count > 0:
    cur = conn.cursor()
    for r in df_media.collect():
        cur.execute("""
            INSERT INTO media_catalog (nasa_id, title, description, media_type, date_created, thumbnail_url, full_url, center)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (nasa_id) DO UPDATE SET
                title = EXCLUDED.title,
                thumbnail_url = EXCLUDED.thumbnail_url,
                full_url = EXCLUDED.full_url
        """, (
            r["nasa_id"], r["title"], r["description"], r["media_type"],
            r["date_created"], r["thumbnail_url"], r["full_url"], r["center"],
        ))
    print(f"Media catalog synced ({media_count} items).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sync Diagnostics
# MAGIC Compute pipeline health from data_quality_log and push to Lakebase.

# COMMAND ----------

df_diag = spark.sql("""
    SELECT
        source,
        CASE
            WHEN SUM(CASE WHEN NOT is_healthy THEN 1 ELSE 0 END) > 3 THEN 'error'
            WHEN SUM(CASE WHEN NOT is_healthy THEN 1 ELSE 0 END) > 0 THEN 'warning'
            ELSE 'healthy'
        END AS health_status,
        MAX(ingest_ts) AS last_ingest_ts,
        CAST(EXTRACT(EPOCH FROM (current_timestamp() - MAX(ingest_ts))) AS INT) AS seconds_since_last_ingest,
        COUNT(*) AS ingests_last_hour,
        SUM(record_count) AS records_last_hour,
        SUM(parse_error_count) AS parse_errors_last_hour,
        AVG(freshness_lag_s) AS avg_freshness_lag_s,
        AVG(latency_ms) AS avg_latency_ms,
        COUNT(DISTINCT schema_hash) AS schema_versions_seen
    FROM artemis_tracker.silver.data_quality_log
    WHERE ingest_ts >= current_timestamp() - INTERVAL 1 HOUR
    GROUP BY source
""")

diag_count = df_diag.count()
print(f"Diagnostics sources to sync: {diag_count}")

if diag_count > 0:
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    for r in df_diag.collect():
        cur.execute("""
            INSERT INTO diagnostics (
                source, health_status, last_ingest_ts,
                seconds_since_last_ingest, ingests_last_hour,
                records_last_hour, parse_errors_last_hour,
                avg_freshness_lag_s, avg_latency_ms,
                schema_versions_seen, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source) DO UPDATE SET
                health_status = EXCLUDED.health_status,
                last_ingest_ts = EXCLUDED.last_ingest_ts,
                seconds_since_last_ingest = EXCLUDED.seconds_since_last_ingest,
                ingests_last_hour = EXCLUDED.ingests_last_hour,
                records_last_hour = EXCLUDED.records_last_hour,
                parse_errors_last_hour = EXCLUDED.parse_errors_last_hour,
                avg_freshness_lag_s = EXCLUDED.avg_freshness_lag_s,
                avg_latency_ms = EXCLUDED.avg_latency_ms,
                schema_versions_seen = EXCLUDED.schema_versions_seen,
                updated_at = EXCLUDED.updated_at
        """, (
            r["source"], r["health_status"], r["last_ingest_ts"],
            int(r["seconds_since_last_ingest"]) if r["seconds_since_last_ingest"] else 0,
            int(r["ingests_last_hour"]),
            int(r["records_last_hour"]) if r["records_last_hour"] else 0,
            int(r["parse_errors_last_hour"]) if r["parse_errors_last_hour"] else 0,
            float(r["avg_freshness_lag_s"]) if r["avg_freshness_lag_s"] else 0.0,
            float(r["avg_latency_ms"]) if r["avg_latency_ms"] else 0.0,
            int(r["schema_versions_seen"]) if r["schema_versions_seen"] else 1,
            now,
        ))
    print(f"Diagnostics synced ({diag_count} sources).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Verify row counts in Lakebase
cur = conn.cursor()
tables = ["current_status", "trajectory_history", "milestones", "media_catalog", "diagnostics"]
print("=" * 50)
print("Lakebase sync complete — row counts:")
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    count = cur.fetchone()[0]
    print(f"  {t:25s} {count:>8,}")
print("=" * 50)

conn.close()
