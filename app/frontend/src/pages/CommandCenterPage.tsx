import React from "react";
import { Link, useLocation } from "react-router-dom";
import usePolling from "../hooks/usePolling";
import DigitalTwin from "../components/DigitalTwin";
import CrewPanel from "../components/CrewPanel";
import DSNPanel from "../components/DSNPanel";
import Timeline from "../components/Timeline";
import OrbitView from "../components/OrbitView";
import MissionAdvisor from "../components/MissionAdvisor";
import StaleBanner from "../components/StaleBanner";
import ErrorBoundary from "../components/ErrorBoundary";

/* ── API response types ─────────────────────────────────────── */

interface CurrentData {
  distance_earth_km: number;
  distance_moon_km: number;
  speed_km_h: number;
  speed_mph: number;
  mission_elapsed_s: number;
  mission_elapsed_display: string;
  phase: string;
  current_phase: string;
  last_milestone: string;
  staleness_seconds: number;
  position: { x_km: number; y_km: number; z_km: number };
  velocity: { vx_km_s: number; vy_km_s: number; vz_km_s: number };
  distance_earth_miles: number;
  distance_moon_miles: number;
}

interface PathPoint {
  x_km: number;
  y_km: number;
  z_km: number;
  epoch_utc: string;
}

interface PathData {
  points: PathPoint[];
}

interface Milestone {
  event: string;
  planned_time: string;
  status: "completed" | "in_progress" | "upcoming";
}

interface MilestonesData {
  milestones: Milestone[];
}

/* ── Helpers ─────────────────────────────────────────────────── */

function formatMET(totalSeconds: number): string {
  const d = Math.floor(totalSeconds / 86400);
  const h = Math.floor((totalSeconds % 86400) / 3600);
  const m = Math.floor((totalSeconds % 3600) / 60);
  const s = Math.floor(totalSeconds % 60);
  return `${d}d ${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function formatNumber(n: number): string {
  return n.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function phaseLabel(phase: string): string {
  const map: Record<string, string> = {
    near_earth: "NEAR EARTH OPS",
    earth_orbit: "EARTH ORBIT",
    transit_out: "OUTBOUND TRANSIT",
    lunar_flyby: "LUNAR FLYBY",
    transit_return: "RETURN TRANSIT",
    reentry: "RE-ENTRY",
  };
  return map[phase] || phase.toUpperCase().replace(/_/g, " ");
}

/* ── Component ──────────────────────────────────────────────── */

const CommandCenterPage: React.FC = () => {
  const location = useLocation();

  const current = usePolling<CurrentData>("/api/v1/current", 30_000);
  const path = usePolling<PathData>("/api/v1/path", 120_000);
  const milestones = usePolling<MilestonesData>("/api/v1/milestones", 300_000);

  const data = current.data;
  const phase = data?.phase || data?.current_phase || "";
  const isLoading = current.loading && path.loading && milestones.loading;
  const isStale = data != null && data.staleness_seconds > 600;

  // Velocity vector for digital twin
  const velocity = data?.velocity
    ? { vx: data.velocity.vx_km_s, vy: data.velocity.vy_km_s, vz: data.velocity.vz_km_s }
    : { vx: -1, vy: 0, vz: 0 };

  const position = data?.position
    ? { x: data.position.x_km, y: data.position.y_km, z: data.position.z_km }
    : { x: 0, y: 0, z: 0 };

  const met = data?.mission_elapsed_s ? formatMET(data.mission_elapsed_s) : "--:--:--:--";

  return (
    <div className="app mcc-app">
      {/* ── NASA-style header bar ──────────────────────────── */}
      <header className="mcc-header">
        <div className="mcc-header-left">
          <div className="mcc-logo">
            <div className="mcc-logo-circle">
              <span>A</span>
              <span className="mcc-logo-ii">II</span>
            </div>
          </div>
          <div className="mcc-header-text">
            <span className="mcc-title">ARTEMIS II</span>
            <span className="mcc-subtitle">MISSION CONTROL</span>
          </div>
        </div>
        <div className="mcc-header-center">
          <div className="mcc-met-block">
            <span className="mcc-met-label">MET</span>
            <span className="mcc-met-value">{met}</span>
          </div>
          <div className="mcc-phase-block">
            <span className={`mcc-phase-indicator ${isStale ? "stale" : "live"}`} />
            <span className="mcc-phase-text">{phase ? phaseLabel(phase) : "ACQUIRING"}</span>
          </div>
        </div>
        <div className="mcc-header-right">
          <div className="mcc-live-badge">
            <span className="mcc-live-dot" />
            LIVE
          </div>
          <Link
            to="/"
            className={`mcc-nav-link${location.pathname === "/" ? " active" : ""}`}
          >
            Mission Control
          </Link>
          <Link
            to="/admin"
            className={`mcc-nav-link${location.pathname === "/admin" ? " active" : ""}`}
          >
            Diagnostics
          </Link>
        </div>
      </header>

      <StaleBanner stalenessSeconds={data?.staleness_seconds ?? 0} visible={isStale} />

      {/* ── Telemetry ticker strip ─────────────────────────── */}
      {data && data.distance_earth_km != null && (
        <div className="mcc-ticker">
          <div className="mcc-ticker-item">
            <span className="mcc-ticker-label">DIST EARTH</span>
            <span className="mcc-ticker-value">{formatNumber(data.distance_earth_km)} km</span>
          </div>
          <div className="mcc-ticker-sep" />
          <div className="mcc-ticker-item">
            <span className="mcc-ticker-label">DIST MOON</span>
            <span className="mcc-ticker-value">{formatNumber(data.distance_moon_km ?? 0)} km</span>
          </div>
          <div className="mcc-ticker-sep" />
          <div className="mcc-ticker-item">
            <span className="mcc-ticker-label">VELOCITY</span>
            <span className="mcc-ticker-value highlight">{formatNumber(data.speed_km_h ?? 0)} km/h</span>
          </div>
          <div className="mcc-ticker-sep" />
          <div className="mcc-ticker-item">
            <span className="mcc-ticker-label">MILESTONE</span>
            <span className="mcc-ticker-value">{data.last_milestone || "—"}</span>
          </div>
          <div className="mcc-ticker-sep" />
          <div className="mcc-ticker-item">
            <span className="mcc-ticker-label">LIGHT DELAY</span>
            <span className="mcc-ticker-value">{(data.distance_earth_km / 299792.458).toFixed(2)}s</span>
          </div>
        </div>
      )}

      {/* ── Main content grid ──────────────────────────────── */}
      <main className={`mcc-main${isStale ? " stale-offset" : ""}`}>
        {isLoading ? (
          <div className="loading-state">
            <span className="loading-spinner" />
            Establishing data link...
          </div>
        ) : (
          <>
            {current.error && (
              <div className="mcc-alert">
                TELEMETRY LINK ERROR: {current.error}
              </div>
            )}

            {/* Row 1: 3D Orbit (large) + Digital Twin (side) */}
            <div className="mcc-row-top">
              <div className="mcc-panel mcc-panel-orbit">
                <div className="mcc-panel-header">
                  <span className="mcc-panel-dot green" />
                  ORBITAL TRACKING
                </div>
                <div className="mcc-orbit-container">
                  <ErrorBoundary>
                    <OrbitView
                      pathData={path.data?.points ?? null}
                      currentPosition={data?.position ?? null}
                      currentVelocity={data?.velocity ?? null}
                      distanceEarthKm={data?.distance_earth_km}
                      distanceMoonKm={data?.distance_moon_km}
                    />
                  </ErrorBoundary>
                </div>
              </div>

              <div className="mcc-panel mcc-panel-twin">
                <div className="mcc-panel-header">
                  <span className="mcc-panel-dot green" />
                  SPACECRAFT ATTITUDE
                </div>
                <ErrorBoundary>
                  <DigitalTwin
                    velocityVector={velocity}
                    position={position}
                    phase={phase}
                    distanceEarthKm={data?.distance_earth_km ?? 0}
                    distanceMoonKm={data?.distance_moon_km ?? 0}
                    missionElapsedDisplay={met}
                  />
                </ErrorBoundary>
              </div>
            </div>

            {/* Row 2: Systems bar */}
            <div className="mcc-systems-bar">
              <div className="mcc-system">
                <span className="mcc-system-dot ok" />
                <span className="mcc-system-label">COMMS</span>
                <span className="mcc-system-status">{isStale ? "STALE" : "NOMINAL"}</span>
              </div>
              <div className="mcc-system">
                <span className="mcc-system-dot ok" />
                <span className="mcc-system-label">NAV</span>
                <span className="mcc-system-status">TRACKING</span>
              </div>
              <div className="mcc-system">
                <span className="mcc-system-dot ok" />
                <span className="mcc-system-label">POWER</span>
                <span className="mcc-system-status">NOMINAL</span>
              </div>
              <div className="mcc-system">
                <span className="mcc-system-dot ok" />
                <span className="mcc-system-label">THERMAL</span>
                <span className="mcc-system-status">NOMINAL</span>
              </div>
              <div className="mcc-system">
                <span className="mcc-system-dot ok" />
                <span className="mcc-system-label">ECLSS</span>
                <span className="mcc-system-status">NOMINAL</span>
              </div>
              <div className="mcc-system">
                <span className="mcc-system-dot ok" />
                <span className="mcc-system-label">CREW 4/4</span>
                <span className="mcc-system-status">ALL GO</span>
              </div>
            </div>

            {/* Row 3: Advisor + DSN + Crew */}
            <div className="mcc-row-bottom">
              <div className="mcc-panel mcc-panel-advisor">
                <div className="mcc-panel-header">
                  <span className="mcc-panel-dot green" />
                  MISSION ADVISOR — POWERED BY GENIE
                </div>
                <MissionAdvisor />
              </div>
              <div className="mcc-panel">
                <DSNPanel distanceEarthKm={data?.distance_earth_km ?? 0} />
              </div>
              <div className="mcc-panel">
                <CrewPanel phase={phase} />
              </div>
            </div>

            {/* Row 4: Timeline at the bottom */}
            <div className="mcc-panel mcc-panel-timeline">
              <Timeline milestones={milestones.data?.milestones ?? null} />
            </div>
          </>
        )}
      </main>
    </div>
  );
};

export default CommandCenterPage;
