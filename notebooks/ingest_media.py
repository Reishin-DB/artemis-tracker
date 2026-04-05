# Databricks notebook source
# MAGIC %md
# MAGIC # Artemis II Tracker — Ingest NASA Media
# MAGIC Polls the NASA Image API for Artemis II imagery.
# MAGIC Writes raw responses to bronze and upserts parsed items to silver.media_catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("page_size", "20", "Number of results per page")
dbutils.widgets.text("search_query", "artemis+II", "NASA Image API search query")

PAGE_SIZE = int(dbutils.widgets.get("page_size"))
SEARCH_QUERY = dbutils.widgets.get("search_query")

# COMMAND ----------

import requests
import uuid
import time
import json
import hashlib
from datetime import datetime, timezone

NASA_IMAGE_API = "https://images-api.nasa.gov/search"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query NASA Image API

# COMMAND ----------

ingest_id = str(uuid.uuid4())
ingest_ts = datetime.now(timezone.utc)

params = {
    "q": SEARCH_QUERY,
    "media_type": "image",
    "page_size": PAGE_SIZE,
}

t0 = time.time()
try:
    resp = requests.get(NASA_IMAGE_API, params=params, timeout=30)
    latency_ms = int((time.time() - t0) * 1000)
    http_status = resp.status_code
    response_json = resp.text
    print(f"NASA Image API: HTTP {http_status}, latency {latency_ms}ms")
except Exception as e:
    latency_ms = int((time.time() - t0) * 1000)
    http_status = -1
    response_json = json.dumps({"error": str(e)})
    print(f"NASA Image API error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Response

# COMMAND ----------

items = []
total_hits = 0

if http_status == 200:
    try:
        data = json.loads(response_json)
        collection = data.get("collection", {})
        total_hits = collection.get("metadata", {}).get("total_hits", 0)
        raw_items = collection.get("items", [])

        for item in raw_items:
            item_data = item.get("data", [{}])[0]
            links = item.get("links", [{}])
            thumbnail_url = links[0].get("href", "") if links else ""

            nasa_id = item_data.get("nasa_id", "")
            if not nasa_id:
                continue

            items.append({
                "nasa_id": nasa_id,
                "title": item_data.get("title", ""),
                "description": (item_data.get("description", "") or "")[:4000],
                "media_type": item_data.get("media_type", "image"),
                "date_created": item_data.get("date_created", ""),
                "thumbnail_url": thumbnail_url,
                "full_url": "",  # would need a second call to the asset manifest
                "center": item_data.get("center", ""),
            })

        print(f"Parsed {len(items)} items from {total_hits} total hits")
    except Exception as e:
        print(f"Parse error: {e}")
else:
    print(f"Skipping parse due to HTTP {http_status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Raw Response to Bronze

# COMMAND ----------

from pyspark.sql import Row

bronze_row = Row(
    ingest_id=ingest_id,
    ingest_ts=ingest_ts,
    api_url=f"{NASA_IMAGE_API}?q={SEARCH_QUERY}&media_type=image&page_size={PAGE_SIZE}",
    http_status=http_status,
    response_json=response_json,
    total_hits=total_hits,
    latency_ms=int(latency_ms),
)

df_bronze = spark.createDataFrame([bronze_row])
df_bronze.write.mode("append").saveAsTable("artemis_tracker.bronze.raw_nasa_media")

print(f"Wrote 1 row to bronze.raw_nasa_media (ingest_id={ingest_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert Parsed Items to Silver (MERGE)

# COMMAND ----------

if items:
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType
    )

    media_rows = []
    for item in items:
        # Parse date_created safely
        date_created = None
        if item["date_created"]:
            try:
                date_created = datetime.fromisoformat(item["date_created"].replace("Z", "+00:00"))
            except Exception:
                pass

        media_rows.append(Row(
            nasa_id=item["nasa_id"],
            title=item["title"],
            description=item["description"],
            media_type=item["media_type"],
            date_created=date_created,
            thumbnail_url=item["thumbnail_url"],
            full_url=item["full_url"],
            center=item["center"],
            first_seen_ts=ingest_ts,
        ))

    df_media = spark.createDataFrame(media_rows)
    df_media.createOrReplaceTempView("staging_media")

    merge_sql = """
    MERGE INTO artemis_tracker.silver.media_catalog AS target
    USING staging_media AS source
    ON target.nasa_id = source.nasa_id
    WHEN MATCHED THEN UPDATE SET
        target.title         = source.title,
        target.description   = source.description,
        target.media_type    = source.media_type,
        target.date_created  = source.date_created,
        target.thumbnail_url = source.thumbnail_url,
        target.full_url      = source.full_url,
        target.center        = source.center
    WHEN NOT MATCHED THEN INSERT (
        nasa_id, title, description, media_type, date_created,
        thumbnail_url, full_url, center, first_seen_ts
    ) VALUES (
        source.nasa_id, source.title, source.description, source.media_type,
        source.date_created, source.thumbnail_url, source.full_url,
        source.center, source.first_seen_ts
    )
    """

    result = spark.sql(merge_sql)
    display(result)
    print(f"Merged {len(items)} media items into silver.media_catalog")
else:
    print("No items to merge.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Data Quality

# COMMAND ----------

quality_row = Row(
    quality_id=str(uuid.uuid4()),
    ingest_ts=ingest_ts,
    source="nasa_image_api",
    ingest_id=ingest_id,
    record_count=len(items),
    parse_error_count=0,
    duplicate_count=0,
    schema_hash=hashlib.md5("media_catalog_v1".encode()).hexdigest(),
    freshness_lag_s=0.0,
    http_status=http_status,
    latency_ms=int(latency_ms),
    is_healthy=(http_status == 200 and len(items) > 0),
)

df_quality = spark.createDataFrame([quality_row])
df_quality.write.mode("append").saveAsTable("artemis_tracker.silver.data_quality_log")

print("Data quality logged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("NASA Media ingest complete.")
print(f"  ingest_id   : {ingest_id}")
print(f"  HTTP status : {http_status}")
print(f"  total_hits  : {total_hits}")
print(f"  items parsed: {len(items)}")
print(f"  latency     : {latency_ms}ms")
print("=" * 60)
