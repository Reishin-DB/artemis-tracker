import React, { useMemo } from "react";

interface DSNPanelProps {
  distanceEarthKm: number;
}

interface DSNStation {
  name: string;
  id: string;
  location: string;
  /** Approximate longitude of the station, used for line-of-sight estimate */
  lonDeg: number;
}

const STATIONS: DSNStation[] = [
  { name: "Goldstone", id: "DSS-14", location: "California, USA", lonDeg: -116.89 },
  { name: "Canberra", id: "DSS-43", location: "Australia", lonDeg: 148.98 },
  { name: "Madrid", id: "DSS-63", location: "Spain", lonDeg: -3.95 },
];

const SPEED_OF_LIGHT_KMS = 299792.458;

/**
 * Rough estimate of which DSN station(s) have line-of-sight to a deep-space
 * target.  Deep-space craft are far enough that any station on the Earth's
 * "day-side" toward the spacecraft can see it.  We approximate by checking
 * if the current UTC hour puts the station in a plausible window.  In reality
 * all three stations are positioned ~120 deg apart so at least one always has
 * visibility — we highlight the primary and mark others as standby.
 */
function estimateStationVisibility(): Map<string, "active" | "standby"> {
  const now = new Date();
  const utcHour = now.getUTCHours() + now.getUTCMinutes() / 60;

  // Each station's "prime" window is roughly when the target is above the
  // local horizon.  For deep-space, each 70m dish covers a wide swath, so
  // we use generous 10-hour windows centered on local midnight (target
  // transits roughly when station is facing away from Sun).
  const result = new Map<string, "active" | "standby">();

  STATIONS.forEach((station) => {
    // Local solar time approximation
    const localSolar = (utcHour + station.lonDeg / 15 + 24) % 24;
    // Deep-space targets transit near local midnight, visible ~17:00-07:00
    const inWindow =
      localSolar >= 17 || localSolar <= 7;
    result.set(station.id, inWindow ? "active" : "standby");
  });

  // Ensure at least one station is active (DSN guarantees coverage)
  const anyActive = Array.from(result.values()).some((v) => v === "active");
  if (!anyActive) {
    // Default to Goldstone
    result.set("DSS-14", "active");
  }

  return result;
}

const DSNPanel: React.FC<DSNPanelProps> = ({ distanceEarthKm }) => {
  const signalDelaySec = distanceEarthKm / SPEED_OF_LIGHT_KMS;
  const roundTripSec = signalDelaySec * 2;

  const visibility = useMemo(() => estimateStationVisibility(), []);

  return (
    <div className="cc-dsn">
      <div className="cc-section-header">DSN Communications</div>
      <div className="cc-dsn-stations">
        {STATIONS.map((station) => {
          const status = visibility.get(station.id) ?? "standby";
          const isActive = status === "active";
          return (
            <div
              className={`cc-dsn-station ${isActive ? "active" : "standby"}`}
              key={station.id}
            >
              <div className="cc-dsn-station-header">
                <span
                  className={`cc-dsn-dot ${isActive ? "active" : "standby"}`}
                />
                <span className="cc-dsn-name">
                  {station.name} {station.id}
                </span>
              </div>
              <div className="cc-dsn-detail">
                <span className="cc-dsn-detail-label">Status</span>
                <span className="cc-dsn-detail-value">
                  {isActive ? "Downlink Active" : "Standby"}
                </span>
              </div>
              <div className="cc-dsn-detail">
                <span className="cc-dsn-detail-label">Location</span>
                <span className="cc-dsn-detail-value">{station.location}</span>
              </div>
              {isActive && (
                <div className="cc-dsn-detail">
                  <span className="cc-dsn-detail-label">Range</span>
                  <span className="cc-dsn-detail-value">
                    {distanceEarthKm.toLocaleString("en-US", {
                      maximumFractionDigits: 0,
                    })}{" "}
                    km
                  </span>
                </div>
              )}
            </div>
          );
        })}
      </div>
      <div className="cc-dsn-signal">
        <div className="cc-dsn-signal-row">
          <span className="cc-dsn-detail-label">One-way Light Time</span>
          <span className="cc-dsn-signal-value">
            {signalDelaySec.toFixed(2)}s
          </span>
        </div>
        <div className="cc-dsn-signal-row">
          <span className="cc-dsn-detail-label">Round-trip Delay</span>
          <span className="cc-dsn-signal-value">
            {roundTripSec.toFixed(2)}s
          </span>
        </div>
      </div>
    </div>
  );
};

export default DSNPanel;
