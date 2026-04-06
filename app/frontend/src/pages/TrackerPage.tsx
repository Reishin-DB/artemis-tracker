import React from "react";
import { Link } from "react-router-dom";
import usePolling from "../hooks/usePolling";
import StatusCards from "../components/StatusCards";
import OrbitView from "../components/OrbitView";
import Timeline from "../components/Timeline";
import MediaPanel from "../components/MediaPanel";
import StaleBanner from "../components/StaleBanner";

interface CurrentData {
  distance_earth_km: number;
  distance_moon_km: number;
  speed_km_h: number;
  mission_elapsed_s: number;
  phase: string;
  staleness_seconds: number;
  position: {
    x_km: number;
    y_km: number;
    z_km: number;
  };
  velocity?: {
    vx_km_s: number;
    vy_km_s: number;
    vz_km_s: number;
  };
  moon_position?: {
    x_km: number;
    y_km: number;
    z_km: number;
  };
}

interface PathPoint {
  x_km: number;
  y_km: number;
  z_km: number;
  epoch_utc: string;
}

interface PathData {
  points: PathPoint[];
  flyby_moon_position?: { x_km: number; y_km: number; z_km: number };
}

interface Milestone {
  event: string;
  planned_time: string;
  status: "completed" | "in_progress" | "upcoming";
}

interface MilestonesData {
  milestones: Milestone[];
}

interface MediaItem {
  title: string;
  url: string;
  thumbnail_url?: string;
  media_type?: string;
  date_created?: string;
}

interface MediaData {
  items: MediaItem[];
}

const TrackerPage: React.FC = () => {
  const current = usePolling<CurrentData>("/api/v1/current", 30_000);
  const path = usePolling<PathData>("/api/v1/path", 120_000);
  const milestones = usePolling<MilestonesData>("/api/v1/milestones", 300_000);
  const media = usePolling<MediaData>("/api/v1/media", 600_000);

  const isStale =
    current.data !== null && current.data.staleness_seconds > 600;
  const stalenessSeconds = current.data?.staleness_seconds ?? 0;

  const isLoading =
    current.loading && path.loading && milestones.loading;

  return (
    <div className="app">
      <nav className="topbar">
        <div className="topbar-left">
          <span className="topbar-title">ARTEMIS II</span>
          <span className="topbar-subtitle">Mission Tracker</span>
        </div>
        <div className="topbar-right">
          <div className="live-indicator">
            <span className="live-dot" />
            Live
          </div>
          <Link to="/" className="topbar-nav-link active">
            Tracker
          </Link>
          <Link to="/command-center" className="topbar-nav-link">
            Command Center
          </Link>
          <Link to="/admin" className="topbar-nav-link">
            Admin
          </Link>
        </div>
      </nav>

      <StaleBanner stalenessSeconds={stalenessSeconds} visible={isStale} />

      <main className={`main-content${isStale ? " stale-offset" : ""}`}>
        {isLoading ? (
          <div className="loading-state">
            <span className="loading-spinner" />
            Acquiring telemetry data...
          </div>
        ) : (
          <>
            {current.error && (
              <div className="error-state">
                Telemetry error: {current.error}
              </div>
            )}

            <StatusCards data={current.data} />

            <OrbitView
              pathData={path.data?.points ?? null}
              currentPosition={current.data?.position ?? null}
              currentVelocity={current.data?.velocity ?? null}
              moonPosition={path.data?.flyby_moon_position ?? current.data?.moon_position}
              distanceEarthKm={current.data?.distance_earth_km}
              distanceMoonKm={current.data?.distance_moon_km}
            />

            <Timeline
              milestones={milestones.data?.milestones ?? null}
            />

            <MediaPanel
              mediaData={media.data?.items ?? null}
            />
          </>
        )}
      </main>
    </div>
  );
};

export default TrackerPage;
