import React from "react";

interface CurrentData {
  distance_earth_km: number;
  distance_moon_km: number;
  speed_km_h: number;
  mission_elapsed_s: number;
  phase: string;
  staleness_seconds: number;
}

interface StatusCardsProps {
  data: CurrentData | null;
}

function kmToMiles(km: number): number {
  return km * 0.621371;
}

function formatElapsedTime(totalSeconds: number): string {
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  return `${days}d ${hours}h ${minutes}m`;
}

function formatNumber(n: number): string {
  return n.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function formatSpeed(n: number): string {
  return n.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

const StatusCards: React.FC<StatusCardsProps> = ({ data }) => {
  if (!data) {
    return (
      <div className="stats-grid">
        {Array.from({ length: 5 }).map((_, i) => (
          <div className="stat-card" key={i}>
            <div className="stat-label">Loading...</div>
            <div className="stat-value" style={{ color: "var(--text-muted)" }}>
              --
            </div>
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="stats-grid">
      <div className="stat-card">
        <div className="stat-label">Distance from Earth</div>
        <div className="stat-value">
          {formatNumber(data.distance_earth_km)}
        </div>
        <div className="stat-unit">km</div>
        <div className="stat-secondary">
          {formatNumber(kmToMiles(data.distance_earth_km))} mi
        </div>
      </div>

      <div className="stat-card">
        <div className="stat-label">Distance from Moon</div>
        <div className="stat-value">
          {formatNumber(data.distance_moon_km)}
        </div>
        <div className="stat-unit">km</div>
        <div className="stat-secondary">
          {formatNumber(kmToMiles(data.distance_moon_km))} mi
        </div>
      </div>

      <div className="stat-card">
        <div className="stat-label">Speed</div>
        <div className="stat-value highlight">
          {formatSpeed(data.speed_km_h)}
        </div>
        <div className="stat-unit">km/h</div>
        <div className="stat-secondary">
          {formatSpeed(kmToMiles(data.speed_km_h))} mph
        </div>
      </div>

      <div className="stat-card">
        <div className="stat-label">Mission Elapsed Time</div>
        <div className="stat-value">
          {formatElapsedTime(data.mission_elapsed_s)}
        </div>
        <div className="stat-unit">
          {Math.floor(data.mission_elapsed_s / 86400)} days into mission
        </div>
      </div>

      <div className="stat-card">
        <div className="stat-label">Current Phase</div>
        <div className="stat-value" style={{ fontSize: "1.2rem" }}>
          <span className="phase-dot active" />
          {data.phase}
        </div>
        <div className="stat-unit">Active</div>
      </div>
    </div>
  );
};

export default StatusCards;
