# Databricks notebook source
# DBTITLE 1,Artemis II Live Data Ingestion
# MAGIC %md
# MAGIC # Artemis II Live Data Ingestion
# MAGIC
# MAGIC This notebook fetches **live Orion spacecraft telemetry** from the [JPL Horizons API](https://ssd.jpl.nasa.gov/horizons/) and writes to both:
# MAGIC
# MAGIC * **Unity Catalog**: `<your-catalog>.<your-schema>` (4 tables)
# MAGIC * **Lakebase**: `<your-lakebase-instance>` / `artemis_app` database (PostgreSQL)
# MAGIC
# MAGIC **Data sources:**
# MAGIC * Orion MPCV position & velocity vectors (Horizons body ID `-1024`)
# MAGIC * Moon position vectors (Horizons body ID `301`) for distance calculations
# MAGIC * NASA Image API for media catalog
# MAGIC
# MAGIC **Tables updated:** `current_status`, `trajectory_history`, `milestones`, `media_catalog`

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install psycopg2-binary requests databricks-sdk --upgrade --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports and configuration
import math
import uuid
import requests
from datetime import datetime, timedelta, timezone
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)
from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
UC_SCHEMA = "<your-catalog>.<your-schema>"
LAKEBASE_INSTANCE = "<your-lakebase-instance>"
LAKEBASE_DB = "artemis_app"
LAUNCH_TIME = datetime(2026, 4, 1, 22, 35, 0, tzinfo=timezone.utc)
HORIZONS_API = "https://ssd.jpl.nasa.gov/api/horizons.api"

print(f"UC Schema:        {UC_SCHEMA}")
print(f"Lakebase:         {LAKEBASE_INSTANCE} / {LAKEBASE_DB}")
print(f"Launch time:      {LAUNCH_TIME.isoformat()}")
print(f"Current UTC:      {datetime.now(timezone.utc).isoformat()}")

# COMMAND ----------

# DBTITLE 1,Lakebase connection helper
import psycopg2
import psycopg2.extras

def get_lakebase_conn():
    """Connect to Lakebase PostgreSQL using Databricks SDK credentials."""
    w = WorkspaceClient()

    # Get instance details for the host
    instance = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
    host = instance.read_write_dns
    print(f"Lakebase host: {host}")

    # Generate short-lived credential
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[LAKEBASE_INSTANCE],
    )
    user = w.current_user.me().user_name

    conn = psycopg2.connect(
        host=host,
        dbname=LAKEBASE_DB,
        user=user,
        password=cred.token,
        sslmode="require",
        connect_timeout=15,
    )
    conn.autocommit = True
    print(f"Connected to Lakebase as {user} @ {LAKEBASE_DB}")
    return conn

# Quick test
_test_conn = get_lakebase_conn()
_test_conn.close()
print("Lakebase connection test passed!")

# COMMAND ----------

# DBTITLE 1,Create Lakebase tables if not exist
def create_lakebase_tables(conn):
    """Create all required tables in Lakebase if they don't already exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS current_status (
        id INTEGER PRIMARY KEY,
        last_update_utc TIMESTAMP,
        mission_elapsed_s DOUBLE PRECISION,
        mission_elapsed_display TEXT,
        current_phase TEXT,
        last_milestone TEXT,
        distance_earth_km DOUBLE PRECISION,
        distance_earth_miles DOUBLE PRECISION,
        distance_moon_km DOUBLE PRECISION,
        distance_moon_miles DOUBLE PRECISION,
        speed_km_h DOUBLE PRECISION,
        speed_mph DOUBLE PRECISION,
        x_km DOUBLE PRECISION,
        y_km DOUBLE PRECISION,
        z_km DOUBLE PRECISION,
        vx_km_s DOUBLE PRECISION,
        vy_km_s DOUBLE PRECISION,
        vz_km_s DOUBLE PRECISION,
        lat_deg DOUBLE PRECISION,
        lon_deg DOUBLE PRECISION,
        altitude_km DOUBLE PRECISION,
        data_source TEXT,
        updated_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS trajectory_history (
        epoch_utc TIMESTAMP,
        mission_elapsed_s DOUBLE PRECISION,
        x_km DOUBLE PRECISION,
        y_km DOUBLE PRECISION,
        z_km DOUBLE PRECISION,
        distance_earth_km DOUBLE PRECISION,
        distance_moon_km DOUBLE PRECISION,
        speed_km_h DOUBLE PRECISION,
        source TEXT,
        PRIMARY KEY (epoch_utc)
    );

    CREATE TABLE IF NOT EXISTS milestones (
        event_name TEXT PRIMARY KEY,
        planned_ts TIMESTAMP,
        actual_ts TIMESTAMP,
        status TEXT,
        phase TEXT,
        description TEXT
    );

    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    print("Lakebase tables created/verified.")

print("create_lakebase_tables() defined.")

# COMMAND ----------

# DBTITLE 1,JPL Horizons API helper
def fetch_horizons_vectors(command, start_time, stop_time, step_size="5 MINUTES"):
    """
    Query JPL Horizons API for position/velocity vectors.
    Returns list of dicts: {epoch_str, x, y, z, vx, vy, vz}
    """
    params = {
        "format": "text",
        "COMMAND": f"'{command}'",
        "EPHEM_TYPE": "'VECTORS'",
        "CENTER": "'500@399'",       # Earth center
        "START_TIME": f"'{start_time}'",
        "STOP_TIME": f"'{stop_time}'",
        "STEP_SIZE": f"'{step_size}'",
        "REF_PLANE": "'FRAME'",
        "VEC_TABLE": "'2'",
        "MAKE_EPHEM": "'YES'",
        "OBJ_DATA": "'NO'",
        "CSV_FORMAT": "'YES'",
    }

    resp = requests.get(HORIZONS_API, params=params, timeout=60)
    resp.raise_for_status()

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
                points.append({
                    "epoch_str": parts[1].strip().replace("A.D. ", ""),
                    "x": float(parts[2]),
                    "y": float(parts[3]),
                    "z": float(parts[4]),
                    "vx": float(parts[5]),
                    "vy": float(parts[6]),
                    "vz": float(parts[7]),
                })
            except (ValueError, IndexError):
                continue

    return points

print("fetch_horizons_vectors() defined.")

# COMMAND ----------

# DBTITLE 1,Fetch full trajectory data
def fetch_full_trajectory():
    """
    Fetch Orion and Moon trajectories from Horizons,
    compute derived fields, return list of trajectory point dicts.
    """
    now = datetime.now(timezone.utc)
    start = "2026-04-02 02:00"
    stop = now.strftime("%Y-%m-%d %H:%M")

    print(f"Fetching Orion trajectory: {start} -> {stop}")
    orion_pts = fetch_horizons_vectors("-1024", start, stop, "5 MINUTES")
    print(f"  Orion points: {len(orion_pts)}")

    print(f"Fetching Moon trajectory (same range)...")
    moon_pts = fetch_horizons_vectors("301", start, stop, "5 MINUTES")
    print(f"  Moon points:  {len(moon_pts)}")

    # Build trajectory with derived fields
    trajectory = []
    for i, op in enumerate(orion_pts):
        x, y, z = op["x"], op["y"], op["z"]
        vx, vy, vz = op["vx"], op["vy"], op["vz"]

        distance_earth_km = math.sqrt(x**2 + y**2 + z**2)
        speed_km_h = math.sqrt(vx**2 + vy**2 + vz**2) * 3600  # km/s -> km/h

        # Moon distance: use matching index if available
        distance_moon_km = 0.0
        if i < len(moon_pts):
            mp = moon_pts[i]
            distance_moon_km = math.sqrt(
                (x - mp["x"])**2 + (y - mp["y"])**2 + (z - mp["z"])**2
            )

        # Parse epoch string to datetime for mission elapsed
        try:
            epoch_dt = datetime.strptime(op["epoch_str"].strip(), "%Y-%b-%d %H:%M:%S.%f")
            epoch_dt = epoch_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                epoch_dt = datetime.strptime(op["epoch_str"].strip(), "%Y-%b-%d %H:%M:%S")
                epoch_dt = epoch_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                epoch_dt = LAUNCH_TIME

        mission_elapsed_s = (epoch_dt - LAUNCH_TIME).total_seconds()

        trajectory.append({
            "epoch_utc": epoch_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "mission_elapsed_s": mission_elapsed_s,
            "x_km": x,
            "y_km": y,
            "z_km": z,
            "distance_earth_km": distance_earth_km,
            "distance_moon_km": distance_moon_km,
            "speed_km_h": speed_km_h,
            "source": "horizons_live",
        })

    print(f"Trajectory built: {len(trajectory)} points")
    return trajectory

print("fetch_full_trajectory() defined.")

# COMMAND ----------

# DBTITLE 1,Fetch current position
def fetch_current_position():
    """
    Fetch the latest Orion position from Horizons and compute all derived fields.
    Returns a dict matching the current_status table schema.
    """
    now = datetime.now(timezone.utc)
    start = (now - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M")
    stop = now.strftime("%Y-%m-%d %H:%M")

    orion_pts = fetch_horizons_vectors("-1024", start, stop, "5 MINUTES")
    if not orion_pts:
        raise RuntimeError("No Orion data returned from Horizons")

    moon_pts = fetch_horizons_vectors("301", start, stop, "5 MINUTES")

    # Take the last (most recent) point
    op = orion_pts[-1]
    x, y, z = op["x"], op["y"], op["z"]
    vx, vy, vz = op["vx"], op["vy"], op["vz"]

    distance_earth_km = math.sqrt(x**2 + y**2 + z**2)
    speed_km_s = math.sqrt(vx**2 + vy**2 + vz**2)
    speed_km_h = speed_km_s * 3600

    # Moon distance
    distance_moon_km = 0.0
    if moon_pts:
        mp = moon_pts[-1]
        distance_moon_km = math.sqrt(
            (x - mp["x"])**2 + (y - mp["y"])**2 + (z - mp["z"])**2
        )

    # Mission elapsed
    elapsed = (now - LAUNCH_TIME).total_seconds()
    d = int(elapsed // 86400)
    h = int((elapsed % 86400) // 3600)
    m = int((elapsed % 3600) // 60)
    elapsed_display = f"{d}d {h}h {m}m"

    # Determine phase based on mission timeline (not distance)
    FLYBY_TIME = datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc)
    RETURN_START = datetime(2026, 4, 7, 0, 0, 0, tzinfo=timezone.utc)
    ENTRY_TIME = datetime(2026, 4, 10, 16, 0, 0, tzinfo=timezone.utc)
    if distance_earth_km < 10000:
        phase = "near_earth"
    elif now < FLYBY_TIME:
        phase = "transit_out"
    elif now < RETURN_START:
        phase = "lunar_flyby"
    elif now < ENTRY_TIME:
        phase = "transit_return"
    else:
        phase = "reentry"

    # Determine last milestone based on phase
    milestone_map = {
        "near_earth": "Launch",
        "transit_out": "Outbound Coast",
        "lunar_flyby": "Lunar Flyby",
        "transit_return": "Return Coast",
    }
    last_milestone = milestone_map.get(phase, "Outbound Coast")

    # Lat/lon (geocentric)
    lat_deg = math.degrees(math.atan2(z, math.sqrt(x**2 + y**2)))
    lon_deg = math.degrees(math.atan2(y, x))
    altitude_km = distance_earth_km - 6371.0  # above Earth surface

    return {
        "id": 1,
        "last_update_utc": now.strftime("%Y-%m-%d %H:%M:%S"),
        "mission_elapsed_s": elapsed,
        "mission_elapsed_display": elapsed_display,
        "current_phase": phase,
        "last_milestone": last_milestone,
        "distance_earth_km": distance_earth_km,
        "distance_earth_miles": distance_earth_km * 0.621371,
        "distance_moon_km": distance_moon_km,
        "distance_moon_miles": distance_moon_km * 0.621371,
        "speed_km_h": speed_km_h,
        "speed_mph": speed_km_h * 0.621371,
        "x_km": x,
        "y_km": y,
        "z_km": z,
        "vx_km_s": vx,
        "vy_km_s": vy,
        "vz_km_s": vz,
        "lat_deg": lat_deg,
        "lon_deg": lon_deg,
        "altitude_km": altitude_km,
        "data_source": "horizons_live",
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
    }

print("fetch_current_position() defined.")

# COMMAND ----------

# DBTITLE 1,Write to Unity Catalog
def write_to_uc(trajectory_points, current_pos):
    """
    Write trajectory and current position data to Unity Catalog tables
    using Spark DataFrames and MERGE statements.
    """
    # --- Trajectory History ---
    print("Writing trajectory to UC...")
    traj_rows = [
        Row(
            epoch_utc=p["epoch_utc"],
            mission_elapsed_s=float(p["mission_elapsed_s"]),
            x_km=float(p["x_km"]),
            y_km=float(p["y_km"]),
            z_km=float(p["z_km"]),
            distance_earth_km=float(p["distance_earth_km"]),
            distance_moon_km=float(p["distance_moon_km"]),
            speed_km_h=float(p["speed_km_h"]),
            source=str(p["source"]),
        )
        for p in trajectory_points
    ]
    if not traj_rows:
        print("WARNING: No trajectory rows to write. Skipping UC trajectory update.")
    else:
        traj_df = spark.createDataFrame(traj_rows)
        traj_df.createOrReplaceTempView("new_trajectory")

        spark.sql(f"""
            MERGE INTO {UC_SCHEMA}.trajectory_history AS target
            USING new_trajectory AS source
            ON target.epoch_utc = source.epoch_utc
            WHEN NOT MATCHED THEN INSERT *
        """)
    new_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {UC_SCHEMA}.trajectory_history").collect()[0]["cnt"]
    print(f"  UC trajectory_history: {new_count} total rows")

    # --- Current Status ---
    print("Writing current status to UC...")
    cs = current_pos
    cs_row = Row(
        id=int(cs["id"]),
        last_update_utc=cs["last_update_utc"],
        mission_elapsed_s=float(cs["mission_elapsed_s"]),
        mission_elapsed_display=str(cs["mission_elapsed_display"]),
        current_phase=str(cs["current_phase"]),
        last_milestone=str(cs["last_milestone"]),
        distance_earth_km=float(cs["distance_earth_km"]),
        distance_earth_miles=float(cs["distance_earth_miles"]),
        distance_moon_km=float(cs["distance_moon_km"]),
        distance_moon_miles=float(cs["distance_moon_miles"]),
        speed_km_h=float(cs["speed_km_h"]),
        speed_mph=float(cs["speed_mph"]),
        x_km=float(cs["x_km"]),
        y_km=float(cs["y_km"]),
        z_km=float(cs["z_km"]),
        vx_km_s=float(cs["vx_km_s"]),
        vy_km_s=float(cs["vy_km_s"]),
        vz_km_s=float(cs["vz_km_s"]),
        lat_deg=float(cs["lat_deg"]),
        lon_deg=float(cs["lon_deg"]),
        altitude_km=float(cs["altitude_km"]),
        data_source=str(cs["data_source"]),
        updated_at=cs["updated_at"],
    )
    if cs_row is None:
        print("WARNING: No current status to write. Skipping.")
        return
    cs_df = spark.createDataFrame([cs_row])
    cs_df.createOrReplaceTempView("new_current_status")

    spark.sql(f"""
        MERGE INTO {UC_SCHEMA}.current_status AS target
        USING new_current_status AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("  UC current_status: upserted (id=1)")

print("write_to_uc() defined.")

# COMMAND ----------

# DBTITLE 1,Write to Lakebase
def write_to_lakebase(conn, trajectory_points, current_pos):
    """
    Write trajectory and current position data to Lakebase PostgreSQL.
    Uses INSERT ON CONFLICT for upsert behavior.
    """
    # --- Trajectory History ---
    print("Writing trajectory to Lakebase...")
    traj_sql = """
        INSERT INTO trajectory_history
            (epoch_utc, mission_elapsed_s, x_km, y_km, z_km,
             distance_earth_km, distance_moon_km, speed_km_h, source)
        VALUES
            (%(epoch_utc)s, %(mission_elapsed_s)s, %(x_km)s, %(y_km)s, %(z_km)s,
             %(distance_earth_km)s, %(distance_moon_km)s, %(speed_km_h)s, %(source)s)
        ON CONFLICT (epoch_utc) DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, traj_sql, trajectory_points, page_size=500)
    print(f"  Lakebase trajectory_history: {len(trajectory_points)} points synced")

    # --- Current Status ---
    print("Writing current status to Lakebase...")
    cs = current_pos
    cs_sql = """
        INSERT INTO current_status
            (id, last_update_utc, mission_elapsed_s, mission_elapsed_display,
             current_phase, last_milestone,
             distance_earth_km, distance_earth_miles,
             distance_moon_km, distance_moon_miles,
             speed_km_h, speed_mph,
             x_km, y_km, z_km, vx_km_s, vy_km_s, vz_km_s,
             lat_deg, lon_deg, altitude_km,
             data_source, updated_at)
        VALUES
            (%(id)s, %(last_update_utc)s, %(mission_elapsed_s)s, %(mission_elapsed_display)s,
             %(current_phase)s, %(last_milestone)s,
             %(distance_earth_km)s, %(distance_earth_miles)s,
             %(distance_moon_km)s, %(distance_moon_miles)s,
             %(speed_km_h)s, %(speed_mph)s,
             %(x_km)s, %(y_km)s, %(z_km)s, %(vx_km_s)s, %(vy_km_s)s, %(vz_km_s)s,
             %(lat_deg)s, %(lon_deg)s, %(altitude_km)s,
             %(data_source)s, %(updated_at)s)
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
            x_km = EXCLUDED.x_km,
            y_km = EXCLUDED.y_km,
            z_km = EXCLUDED.z_km,
            vx_km_s = EXCLUDED.vx_km_s,
            vy_km_s = EXCLUDED.vy_km_s,
            vz_km_s = EXCLUDED.vz_km_s,
            lat_deg = EXCLUDED.lat_deg,
            lon_deg = EXCLUDED.lon_deg,
            altitude_km = EXCLUDED.altitude_km,
            data_source = EXCLUDED.data_source,
            updated_at = EXCLUDED.updated_at
    """
    with conn.cursor() as cur:
        cur.execute(cs_sql, cs)
    print("  Lakebase current_status: upserted (id=1)")

print("write_to_lakebase() defined.")

# COMMAND ----------

# DBTITLE 1,Update milestones
def update_milestones(conn):
    """
    Write the 9 Artemis II mission milestones to both Lakebase and UC.
    Computes status dynamically based on current time.
    """
    now = datetime.now(timezone.utc)

    MISSION_MILESTONES = [
        {"event_name": "Launch", "planned_ts": "2026-04-01 22:35:00", "actual_ts": "2026-04-01 22:35:00", "phase": "launch", "description": "SLS liftoff from KSC LC-39B"},
        {"event_name": "ICPS Separation", "planned_ts": "2026-04-02 00:35:00", "actual_ts": "2026-04-02 00:35:00", "phase": "earth_orbit", "description": "Interim Cryogenic Propulsion Stage separates from Orion"},
        {"event_name": "Perigee Raise Burn", "planned_ts": "2026-04-02 08:00:00", "actual_ts": "2026-04-02 08:12:00", "phase": "earth_orbit", "description": "Service module engine burn to raise orbit"},
        {"event_name": "Trans-Lunar Injection", "planned_ts": "2026-04-02 10:00:00", "actual_ts": "2026-04-02 10:00:00", "phase": "transit_out", "description": "ICPS burn sends Orion toward the Moon"},
        {"event_name": "Outbound Coast", "planned_ts": "2026-04-02 14:00:00", "actual_ts": "2026-04-02 14:00:00", "phase": "transit_out", "description": "Free-flight trajectory toward the Moon"},
        {"event_name": "Lunar Flyby", "planned_ts": "2026-04-06 12:00:00", "actual_ts": None, "phase": "lunar_flyby", "description": "Closest approach ~4,000 miles from lunar surface"},
        {"event_name": "Return Coast", "planned_ts": "2026-04-07 00:00:00", "actual_ts": None, "phase": "transit_return", "description": "Free-return trajectory back to Earth"},
        {"event_name": "Entry Interface", "planned_ts": "2026-04-10 16:00:00", "actual_ts": None, "phase": "reentry", "description": "Orion capsule enters Earth atmosphere at 25,000 mph"},
        {"event_name": "Splashdown", "planned_ts": "2026-04-10 17:00:00", "actual_ts": None, "phase": "reentry", "description": "Pacific Ocean recovery"},
    ]

    # Compute status for each milestone
    for ms in MISSION_MILESTONES:
        if ms["actual_ts"]:
            ms["status"] = "completed"
        else:
            planned_dt = datetime.strptime(ms["planned_ts"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            ms["status"] = "in_progress" if planned_dt <= now else "upcoming"

    # --- Write to Lakebase ---
    print("Writing milestones to Lakebase...")
    ms_sql = """
        INSERT INTO milestones (event_name, planned_ts, actual_ts, status, phase, description)
        VALUES (%(event_name)s, %(planned_ts)s, %(actual_ts)s, %(status)s, %(phase)s, %(description)s)
        ON CONFLICT (event_name) DO UPDATE SET
            planned_ts = EXCLUDED.planned_ts,
            actual_ts = EXCLUDED.actual_ts,
            status = EXCLUDED.status,
            phase = EXCLUDED.phase,
            description = EXCLUDED.description
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, ms_sql, MISSION_MILESTONES)
    print(f"  Lakebase milestones: {len(MISSION_MILESTONES)} upserted")

    # --- Write to UC ---
    print("Writing milestones to UC...")
    ms_rows = [
        Row(
            event_name=ms["event_name"],
            planned_ts=ms["planned_ts"],
            actual_ts=ms["actual_ts"],
            status=ms["status"],
            phase=ms["phase"],
            description=ms["description"],
        )
        for ms in MISSION_MILESTONES
    ]
    ms_df = spark.createDataFrame(ms_rows)
    ms_df.createOrReplaceTempView("new_milestones")

    spark.sql(f"""
        MERGE INTO {UC_SCHEMA}.milestones AS target
        USING new_milestones AS source
        ON target.event_name = source.event_name
        WHEN MATCHED THEN UPDATE SET
            target.planned_ts = source.planned_ts,
            target.actual_ts = source.actual_ts,
            target.status = source.status,
            target.phase = source.phase,
            target.description = source.description
        WHEN NOT MATCHED THEN INSERT (event_name, planned_ts, actual_ts, status, phase, description)
            VALUES (source.event_name, source.planned_ts, source.actual_ts, source.status, source.phase, source.description)
    """)
    print("  UC milestones: merged")

print("update_milestones() defined.")

# COMMAND ----------

# DBTITLE 1,Update media catalog
def update_media(conn):
    """
    Fetch Artemis II images from NASA Image API and write to both Lakebase and UC.
    """
    print("Fetching media from NASA Image API...")
    try:
        resp = requests.get("https://images-api.nasa.gov/search", params={
            "q": "artemis II 2026",
            "media_type": "image",
            "page_size": 20,
            "year_start": "2026",
        }, timeout=15)
        items = resp.json().get("collection", {}).get("items", [])
    except Exception as e:
        print(f"  NASA API fetch failed: {e}")
        items = []

    media_list = []
    for item in items[:20]:
        d = item.get("data", [{}])[0]
        links = item.get("links", [])
        thumb = links[0].get("href", "") if links else ""
        date_str = d.get("date_created", "")

        # Parse date
        date_created = None
        if date_str:
            try:
                date_created = datetime.fromisoformat(date_str.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                date_created = None

        media_list.append({
            "nasa_id": d.get("nasa_id", ""),
            "title": d.get("title", ""),
            "media_type": d.get("media_type", "image"),
            "thumbnail_url": thumb,
            "full_url": thumb.replace("~thumb", "~orig") if thumb else "",
            "date_created": date_created,
        })

    print(f"  Found {len(media_list)} media items")

    if not media_list:
        print("  No media to write.")
        return

    # --- Write to Lakebase ---
    print("Writing media to Lakebase...")
    media_sql = """
        INSERT INTO media_catalog (nasa_id, title, media_type, thumbnail_url, full_url, date_created)
        VALUES (%(nasa_id)s, %(title)s, %(media_type)s, %(thumbnail_url)s, %(full_url)s, %(date_created)s)
        ON CONFLICT (nasa_id) DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, media_sql, media_list)
    print(f"  Lakebase media_catalog: {len(media_list)} synced")

    # --- Write to UC ---
    print("Writing media to UC...")
    media_rows = [
        Row(
            nasa_id=m["nasa_id"],
            title=m["title"],
            media_type=m["media_type"],
            thumbnail_url=m["thumbnail_url"],
            full_url=m["full_url"],
            date_created=m["date_created"],
        )
        for m in media_list
    ]
    media_df = spark.createDataFrame(media_rows)
    media_df.createOrReplaceTempView("new_media")

    spark.sql(f"""
        MERGE INTO {UC_SCHEMA}.media_catalog AS target
        USING new_media AS source
        ON target.nasa_id = source.nasa_id
        WHEN NOT MATCHED THEN INSERT (nasa_id, title, media_type, thumbnail_url, full_url, date_created)
            VALUES (source.nasa_id, source.title, source.media_type, source.thumbnail_url, source.full_url, source.date_created)
    """)
    print("  UC media_catalog: merged")

print("update_media() defined.")

# COMMAND ----------

# DBTITLE 1,Run full ingestion
# ============================================================
# MAIN EXECUTION: Fetch live data and sync to UC + Lakebase
# ============================================================

print("=" * 60)
print("ARTEMIS II LIVE DATA INGESTION")
print("=" * 60)

# 1. Fetch trajectory from JPL Horizons
print("\n[1/6] Fetching full trajectory from JPL Horizons...")
trajectory = fetch_full_trajectory()
print(f"       {len(trajectory)} trajectory points retrieved")

# 2. Fetch current position
print("\n[2/6] Fetching current Orion position...")
current = fetch_current_position()
print(f"       Phase: {current['current_phase']}")
print(f"       Distance from Earth: {current['distance_earth_km']:,.0f} km")
print(f"       Distance from Moon:  {current['distance_moon_km']:,.0f} km")
print(f"       Speed: {current['speed_km_h']:,.0f} km/h")
print(f"       Elapsed: {current['mission_elapsed_display']}")

# 3. Connect to Lakebase and create tables
print("\n[3/6] Connecting to Lakebase...")
conn = get_lakebase_conn()
create_lakebase_tables(conn)

# 4. Write to Unity Catalog
print("\n[4/6] Writing to Unity Catalog...")
write_to_uc(trajectory, current)

# 5. Write to Lakebase
print("\n[5/6] Writing to Lakebase...")
write_to_lakebase(conn, trajectory, current)

# 6. Update milestones
print("\n[6/6] Updating milestones...")
update_milestones(conn)
# media_catalog removed — not used in the app

# Cleanup
conn.close()

print("\n" + "=" * 60)
print("INGESTION COMPLETE")
print(f"  Trajectory points: {len(trajectory)}")
print(f"  Current phase:     {current['current_phase']}")
print(f"  UC + Lakebase:     SYNCED")
print("=" * 60)