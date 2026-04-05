# Databricks notebook source
# MAGIC %md
# MAGIC # Artemis II Tracker — Seed Milestones
# MAGIC Inserts known Artemis II mission milestones into `silver.mission_events`.
# MAGIC Uses MERGE for idempotency — safe to re-run.

# COMMAND ----------

import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Milestones

# COMMAND ----------

milestones = [
    {
        "event_id": "milestone-launch",
        "event_name": "Launch",
        "planned_ts": "2026-04-01T22:35:00Z",
        "phase": "launch",
        "is_completed": True,
        "description": "SLS Block 1 lifts off from LC-39B, Kennedy Space Center.",
    },
    {
        "event_id": "milestone-icps-separation",
        "event_name": "ICPS Separation",
        "planned_ts": "2026-04-02T00:35:00Z",
        "phase": "earth_orbit",
        "is_completed": True,
        "description": "Interim Cryogenic Propulsion Stage separates from Orion after TLI insertion.",
    },
    {
        "event_id": "milestone-perigee-raise",
        "event_name": "Perigee Raise Burn",
        "planned_ts": "2026-04-02T08:00:00Z",
        "phase": "earth_orbit",
        "is_completed": True,
        "description": "Orion performs perigee raise maneuver to adjust orbit.",
    },
    {
        "event_id": "milestone-tli-burn",
        "event_name": "TLI Burn",
        "planned_ts": "2026-04-02T14:00:00Z",
        "phase": "transit_out",
        "is_completed": True,
        "description": "Trans-Lunar Injection burn sends Orion on a trajectory toward the Moon.",
    },
    {
        "event_id": "milestone-outbound-coast",
        "event_name": "Outbound Coast",
        "planned_ts": "2026-04-03T00:00:00Z",
        "phase": "transit_out",
        "is_completed": True,
        "description": "Orion coasts toward the Moon with periodic navigation checks.",
    },
    {
        "event_id": "milestone-lunar-flyby",
        "event_name": "Lunar Flyby",
        "planned_ts": "2026-04-06T23:02:00Z",
        "phase": "lunar_flyby",
        "is_completed": False,
        "description": "Orion performs powered flyby ~100 km above the lunar far side.",
    },
    {
        "event_id": "milestone-return-coast",
        "event_name": "Return Coast",
        "planned_ts": "2026-04-07T12:00:00Z",
        "phase": "transit_return",
        "is_completed": False,
        "description": "Orion coasts back toward Earth after lunar flyby.",
    },
    {
        "event_id": "milestone-entry-interface",
        "event_name": "Entry Interface",
        "planned_ts": "2026-04-10T16:00:00Z",
        "phase": "reentry",
        "is_completed": False,
        "description": "Orion enters Earth atmosphere at ~40,000 km/h, beginning skip reentry.",
    },
    {
        "event_id": "milestone-splashdown",
        "event_name": "Splashdown",
        "planned_ts": "2026-04-10T18:00:00Z",
        "phase": "reentry",
        "is_completed": False,
        "description": "Orion splashes down in the Pacific Ocean, completing the mission.",
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Temp View and MERGE

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType
)
from datetime import datetime

rows = []
for m in milestones:
    rows.append(Row(
        event_id=m["event_id"],
        event_ts=datetime.fromisoformat(m["planned_ts"].replace("Z", "+00:00")),
        event_type="milestone",
        event_name=m["event_name"],
        description=m["description"],
        source="seed_milestones",
        phase=m["phase"],
        is_completed=m["is_completed"],
        planned_ts=datetime.fromisoformat(m["planned_ts"].replace("Z", "+00:00")),
        actual_ts=datetime.fromisoformat(m["planned_ts"].replace("Z", "+00:00")) if m["is_completed"] else None,
    ))

df = spark.createDataFrame(rows)
df.createOrReplaceTempView("staging_milestones")

# COMMAND ----------

merge_sql = """
MERGE INTO artemis_tracker.silver.mission_events AS target
USING staging_milestones AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET
    target.event_ts      = source.event_ts,
    target.event_type    = source.event_type,
    target.event_name    = source.event_name,
    target.description   = source.description,
    target.source        = source.source,
    target.phase         = source.phase,
    target.is_completed  = source.is_completed,
    target.planned_ts    = source.planned_ts,
    target.actual_ts     = source.actual_ts
WHEN NOT MATCHED THEN INSERT (
    event_id, event_ts, event_type, event_name, description,
    source, phase, is_completed, planned_ts, actual_ts
) VALUES (
    source.event_id, source.event_ts, source.event_type, source.event_name,
    source.description, source.source, source.phase, source.is_completed,
    source.planned_ts, source.actual_ts
)
"""

result = spark.sql(merge_sql)
display(result)

# COMMAND ----------

# Verify
count = spark.sql("SELECT COUNT(*) AS cnt FROM artemis_tracker.silver.mission_events WHERE event_type = 'milestone'").collect()[0]["cnt"]
print(f"Total milestones in mission_events: {count}")

# COMMAND ----------

display(spark.sql("SELECT event_id, event_name, phase, is_completed, planned_ts FROM artemis_tracker.silver.mission_events WHERE event_type='milestone' ORDER BY planned_ts"))
