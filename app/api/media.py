"""
/api/v1/media — NASA media catalog and live-stream link.
"""

import logging
from typing import Any

import requests
from fastapi import APIRouter

from app.cache import cached
from app.db import execute_query, get_backend, table

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["media"])

# NASA TV live — channel embed auto-plays current live stream
NASA_TV_EMBED = "https://www.youtube.com/embed/live_stream?channel=UCLA_DiR1FfKNvjuUpBHmylQ"


def _fetch_media_from_nasa_api() -> list[dict]:
    """Fetch Artemis II images directly from NASA Image API."""
    try:
        resp = requests.get("https://images-api.nasa.gov/search", params={
            "q": "artemis II 2026", "media_type": "image", "page_size": 20,
            "year_start": "2026",
        }, timeout=15)
        items = resp.json().get("collection", {}).get("items", [])
        result = []
        for item in items[:20]:
            d = item.get("data", [{}])[0]
            links = item.get("links", [])
            thumb = links[0].get("href", "") if links else ""
            result.append({
                "nasa_id": d.get("nasa_id", ""),
                "title": d.get("title", ""),
                "media_type": d.get("media_type", "image"),
                "thumbnail_url": thumb,
                "full_url": thumb.replace("~thumb", "~orig") if thumb else "",
                "date_created": d.get("date_created"),
            })
        return result
    except Exception as e:
        logger.warning("NASA Image API fetch failed: %s", e)
        return []


@cached(ttl_seconds=600)
def _fetch_media() -> dict[str, Any]:
    backend = get_backend()

    if backend in ("postgres", "databricks"):
        sql = f"SELECT nasa_id, title, media_type, thumbnail_url, full_url, date_created FROM {table('media_catalog')} ORDER BY date_created DESC LIMIT 20"
        try:
            rows = execute_query(sql)
            if rows:
                items = [
                    {
                        "nasa_id": r.get("nasa_id", ""),
                        "title": r.get("title", ""),
                        "media_type": r.get("media_type", ""),
                        "thumbnail_url": r.get("thumbnail_url", ""),
                        "full_url": r.get("full_url", ""),
                        "date_created": r.get("date_created"),
                    }
                    for r in rows
                ]
                return {"items": items, "nasa_tv_embed": NASA_TV_EMBED}
        except Exception:
            logger.exception("Failed to query media_catalog")

    # Fallback: fetch directly from NASA API
    items = _fetch_media_from_nasa_api()
    return {"items": items, "nasa_tv_embed": NASA_TV_EMBED}


@router.get("/media")
async def get_media():
    return _fetch_media()
