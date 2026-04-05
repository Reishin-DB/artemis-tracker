"""
/api/v1/advisor — LLM-powered Mission Advisor using Databricks Foundation Model API.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["advisor"])

LAUNCH_TIME = datetime(2026, 4, 1, 22, 35, 0, tzinfo=timezone.utc)

MISSION_CONTEXT = """You are the Artemis II Mission Advisor — an expert AI assistant embedded in the Mission Control Center for NASA's Artemis II mission.

MISSION FACTS:
- Artemis II launched April 1, 2026 at 22:35 UTC from KSC LC-39B on SLS Block 1
- Crew: Commander Reid Wiseman, Pilot Victor Glover, MS1 Christina Koch (all NASA), MS2 Jeremy Hansen (CSA)
- First crewed mission beyond low Earth orbit since Apollo 17 (December 1972)
- ~10-day free-return trajectory: Earth orbit → TLI → lunar flyby at ~6,400 km (~4,000 mi) → return → splashdown
- Spacecraft: Orion MPCV with European Service Module (ESM)

TIMELINE:
- Flight Day 1 (Apr 1): Launch, ICPS separation, 2 Earth orbits
- Flight Day 2 (Apr 2): Perigee Raise Burn, Trans-Lunar Injection burn, ICPS sep, outbound coast begins
- Flight Day 3 (Apr 3): Outbound coast, trajectory correction burn, crew Earth photography
- Flight Day 4 (Apr 4): Deep-space transit, manual piloting demonstration, cabin prep for flyby
- Flight Day 5 (Apr 5): Approaching lunar sphere of influence, pre-flyby checks
- Flight Day 6 (Apr 6): LUNAR FLYBY — closest approach ~6,400 km from surface, far-side observation window 2:45-9:40 PM EDT
- Flight Day 7 (Apr 7): Post-flyby, return trajectory established, crew rest
- Flight Days 8-9 (Apr 8-9): Return coast, re-entry prep, communication tests
- Flight Day 10 (Apr 10): Entry interface at 25,000 mph, parachute deploy, Pacific splashdown

SPACECRAFT SYSTEMS:
- Orion crew module: 4 crew, life support for 21 days
- European Service Module: main engine (AJ10-190), 4 solar arrays generating 11kW
- Navigation: star trackers, IMU, DSN tracking via Goldstone/Canberra/Madrid
- Communications: S-band (voice/telemetry), Ka-band (high-rate data), Optical comm demo
- Heat shield: AVCOAT ablative, rated for 2,760°C re-entry

DSN COMMUNICATIONS:
- Three complexes 120° apart ensure continuous coverage
- Goldstone (California), Canberra (Australia), Madrid (Spain)
- Light delay at lunar distance: ~1.3 seconds one-way

You have access to live telemetry data injected below. Use it to give precise, current answers. Be concise but thorough. Use mission-specific terminology. If asked about future events, reference the timeline. If asked about past events, describe what happened based on the flight day logs."""


class ChatRequest(BaseModel):
    message: str
    history: list[dict[str, str]] = []


def _get_live_context() -> str:
    """Build live telemetry context string from current API data."""
    from app.api.current import _fetch_current
    from app.api.milestones import _fetch_milestones

    lines = []
    try:
        current = _fetch_current()
        if current and not current.get("error"):
            now = datetime.now(timezone.utc)
            elapsed = (now - LAUNCH_TIME).total_seconds()
            flight_day = int(elapsed // 86400) + 1
            lines.append(f"CURRENT TIME: {now.strftime('%Y-%m-%d %H:%M UTC')}")
            lines.append(f"FLIGHT DAY: {flight_day}")
            lines.append(f"MISSION ELAPSED: {current.get('mission_elapsed_display', 'unknown')}")
            lines.append(f"PHASE: {current.get('phase', current.get('current_phase', 'unknown'))}")
            lines.append(f"DISTANCE FROM EARTH: {current.get('distance_earth_km', 0):,.0f} km ({current.get('distance_earth_miles', 0):,.0f} mi)")
            lines.append(f"DISTANCE FROM MOON: {current.get('distance_moon_km', 0):,.0f} km ({current.get('distance_moon_miles', 0):,.0f} mi)")
            lines.append(f"VELOCITY: {current.get('speed_km_h', 0):,.0f} km/h ({current.get('speed_mph', 0):,.0f} mph)")
            pos = current.get("position", {})
            if pos:
                lines.append(f"POSITION (J2000 ECI): X={pos.get('x_km',0):,.1f} Y={pos.get('y_km',0):,.1f} Z={pos.get('z_km',0):,.1f} km")
    except Exception as e:
        lines.append(f"TELEMETRY UNAVAILABLE: {e}")

    try:
        ms = _fetch_milestones()
        completed = [m["event"] for m in ms.get("milestones", []) if m.get("status") == "completed"]
        upcoming = [m["event"] for m in ms.get("milestones", []) if m.get("status") in ("upcoming", "in_progress")]
        if completed:
            lines.append(f"COMPLETED MILESTONES: {', '.join(completed)}")
        if upcoming:
            lines.append(f"UPCOMING MILESTONES: {', '.join(upcoming)}")
    except Exception:
        pass

    return "\n".join(lines)


@router.post("/advisor")
async def chat_with_advisor(req: ChatRequest):
    """Stream a response from the Mission Advisor LLM."""
    import requests as http_requests

    live_context = _get_live_context()
    system_prompt = f"{MISSION_CONTEXT}\n\nLIVE TELEMETRY:\n{live_context}"

    messages = [{"role": "system", "content": system_prompt}]
    for msg in req.history[-10:]:  # last 10 messages for context
        messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
    messages.append({"role": "user", "content": req.message})

    # Use Databricks Foundation Model API
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        host = w.config.host
        token = w.config.token

        model = os.environ.get("ADVISOR_MODEL", "databricks-claude-sonnet-4-20250514")

        def generate():
            resp = http_requests.post(
                f"{host}/serving-endpoints/{model}/invocations",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={
                    "messages": messages,
                    "max_tokens": 1024,
                    "temperature": 0.7,
                    "stream": True,
                },
                stream=True,
                timeout=60,
            )
            for line in resp.iter_lines():
                if line:
                    decoded = line.decode("utf-8")
                    if decoded.startswith("data: "):
                        data = decoded[6:]
                        if data.strip() == "[DONE]":
                            break
                        try:
                            chunk = json.loads(data)
                            delta = chunk.get("choices", [{}])[0].get("delta", {})
                            content = delta.get("content", "")
                            if content:
                                yield f"data: {json.dumps({'content': content})}\n\n"
                        except json.JSONDecodeError:
                            pass
            yield "data: [DONE]\n\n"

        return StreamingResponse(generate(), media_type="text/event-stream")

    except Exception as e:
        logger.error("Advisor LLM call failed: %s", e)
        return {"error": str(e)}
