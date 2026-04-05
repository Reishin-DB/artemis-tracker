"""
Artemis II Mission Tracker — FastAPI backend.
Serves the React SPA and exposes /api/v1/* data endpoints.
"""

import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api import advisor, current, diagnostics, media, milestones, path

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
APP_TITLE = os.getenv("APP_TITLE", "Artemis II Mission Tracker")

app = FastAPI(
    title=APP_TITLE,
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------
app.include_router(current.router)
app.include_router(path.router)
app.include_router(milestones.router)
app.include_router(media.router)
app.include_router(diagnostics.router)
app.include_router(advisor.router)


@app.get("/api/health", tags=["health"])
async def health_check():
    """Lightweight health probe with backend info."""
    from app.db import get_backend_info
    info = get_backend_info()
    return {
        "status": "ok",
        "app": APP_TITLE,
        **info,
        "env_pghost": os.environ.get("PGHOST", "NOT SET")[:40],
        "env_lakebase": os.environ.get("LAKEBASE_INSTANCE", "NOT SET"),
    }


# ---------------------------------------------------------------------------
# SPA static files
# ---------------------------------------------------------------------------
STATIC_DIR = Path(__file__).resolve().parent / "static"

ASSETS_DIR = STATIC_DIR / "assets"
if ASSETS_DIR.is_dir():
    app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")

if STATIC_DIR.is_dir():
    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(request: Request, full_path: str):
        """Catch-all: serve index.html for any non-API route (SPA routing)."""
        # If the requested file exists on disk, serve it directly
        file_path = STATIC_DIR / full_path
        if full_path and file_path.is_file():
            return FileResponse(str(file_path))
        # Otherwise, fall back to index.html for client-side routing
        index = STATIC_DIR / "index.html"
        if index.is_file():
            return FileResponse(str(index))
        return JSONResponse({"error": "Frontend not built yet. Run the React build first."}, status_code=503)
