import React from "react";
import SpaceView from "./SpaceView";

interface OrbitViewProps {
  pathData: Array<{
    x_km: number;
    y_km: number;
    z_km: number;
    epoch_utc: string;
  }> | null;
  currentPosition: { x_km: number; y_km: number; z_km: number } | null;
  currentVelocity?: {
    vx_km_s: number;
    vy_km_s: number;
    vz_km_s: number;
  } | null;
  moonPosition?: { x_km: number; y_km: number; z_km: number };
  distanceEarthKm?: number;
  distanceMoonKm?: number;
}

const OrbitView: React.FC<OrbitViewProps> = ({
  pathData,
  currentPosition,
  currentVelocity,
  moonPosition,
  distanceEarthKm,
  distanceMoonKm,
}) => {
  return (
    <div style={{ width: "100%", height: "100%", minHeight: 500 }}>
      <SpaceView
        trajectoryPoints={pathData ?? []}
        currentPosition={currentPosition}
        currentVelocity={currentVelocity ?? null}
        moonPosition={moonPosition}
        distanceEarthKm={distanceEarthKm}
        distanceMoonKm={distanceMoonKm}
      />
    </div>
  );
};

export default OrbitView;
