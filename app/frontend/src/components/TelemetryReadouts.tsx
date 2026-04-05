import React from "react";

interface TelemetryReadoutsProps {
  distanceEarthKm: number | null;
  distanceMoonKm: number | null;
  speedKmh: number | null;
  position: { x: number; y: number; z: number } | null;
  missionElapsedSeconds: number | null;
}

const SPEED_OF_LIGHT_KMS = 299792.458;

function formatNum(n: number, decimals = 0): string {
  return n.toLocaleString("en-US", { maximumFractionDigits: decimals });
}

/** Rough latitude/longitude from Earth-centered XYZ */
function computeLatLon(pos: { x: number; y: number; z: number }) {
  const r = Math.sqrt(pos.x ** 2 + pos.y ** 2 + pos.z ** 2);
  if (r === 0) return { lat: 0, lon: 0, alt: 0 };
  const lat = (Math.asin(pos.z / r) * 180) / Math.PI;
  const lon = (Math.atan2(pos.y, pos.x) * 180) / Math.PI;
  return { lat, lon, alt: r };
}

const TelemetryReadouts: React.FC<TelemetryReadoutsProps> = ({
  distanceEarthKm,
  distanceMoonKm,
  speedKmh,
  position,
  missionElapsedSeconds,
}) => {
  const latLon = position ? computeLatLon(position) : null;
  const lightDelay =
    distanceEarthKm != null ? distanceEarthKm / SPEED_OF_LIGHT_KMS : null;
  const altKm = latLon ? latLon.alt : distanceEarthKm;

  const readouts: { label: string; value: string; unit: string }[] = [
    {
      label: "Altitude",
      value: altKm != null ? formatNum(altKm) : "--",
      unit: "km",
    },
    {
      label: "Speed",
      value: speedKmh != null ? formatNum(speedKmh) : "--",
      unit: "km/h",
    },
    {
      label: "Earth Dist",
      value: distanceEarthKm != null ? formatNum(distanceEarthKm) : "--",
      unit: "km",
    },
    {
      label: "Moon Dist",
      value: distanceMoonKm != null ? formatNum(distanceMoonKm) : "--",
      unit: "km",
    },
    {
      label: "Latitude",
      value: latLon != null ? `${latLon.lat >= 0 ? "+" : ""}${latLon.lat.toFixed(1)}` : "--",
      unit: "\u00B0",
    },
    {
      label: "Longitude",
      value: latLon != null ? `${latLon.lon >= 0 ? "+" : ""}${latLon.lon.toFixed(1)}` : "--",
      unit: "\u00B0",
    },
    {
      label: "Light Delay",
      value: lightDelay != null ? lightDelay.toFixed(2) : "--",
      unit: "s",
    },
    {
      label: "MET",
      value:
        missionElapsedSeconds != null
          ? formatMET(missionElapsedSeconds)
          : "--",
      unit: "",
    },
  ];

  return (
    <div className="cc-telemetry">
      <div className="cc-section-header">Telemetry Readouts</div>
      <div className="cc-telem-grid">
        {readouts.map((r) => (
          <div className="cc-telem-cell" key={r.label}>
            <span className="cc-telem-label">{r.label}</span>
            <span className="cc-telem-value">
              {r.value}
              {r.unit && <span className="cc-telem-unit">{r.unit}</span>}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
};

function formatMET(totalSeconds: number): string {
  const d = Math.floor(totalSeconds / 86400);
  const h = Math.floor((totalSeconds % 86400) / 3600);
  const m = Math.floor((totalSeconds % 3600) / 60);
  const s = Math.floor(totalSeconds % 60);
  return `${d}d ${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

export default TelemetryReadouts;
