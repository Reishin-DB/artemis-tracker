import React, { useRef, useMemo } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import { OrbitControls, Html, Line } from "@react-three/drei";
import * as THREE from "three";

/* ------------------------------------------------------------------ */
/*  Scale: 1 scene-unit = 10 000 km                                   */
/* ------------------------------------------------------------------ */
const SCALE = 20_000;
const toScene = (km: number) => km / SCALE;

const EARTH_RADIUS = toScene(6_371);   // 0.637
const MOON_RADIUS = toScene(1_737);    // 0.174

/* ------------------------------------------------------------------ */
/*  Prop types                                                         */
/* ------------------------------------------------------------------ */
export interface SpaceViewProps {
  trajectoryPoints: Array<{
    x_km: number;
    y_km: number;
    z_km: number;
    epoch_utc: string;
  }>;
  currentPosition: { x_km: number; y_km: number; z_km: number } | null;
  currentVelocity: {
    vx_km_s: number;
    vy_km_s: number;
    vz_km_s: number;
  } | null;
  moonPosition?: { x_km: number; y_km: number; z_km: number };
  distanceEarthKm?: number;
  distanceMoonKm?: number;
}

/* ================================================================== */
/*  Sub-components rendered inside <Canvas>                            */
/* ================================================================== */

/* ---------- Starfield -------------------------------------------- */
function Starfield({ count = 2500 }: { count?: number }) {
  const geometry = useMemo(() => {
    const geo = new THREE.BufferGeometry();
    const positions = new Float32Array(count * 3);
    for (let i = 0; i < count; i++) {
      const theta = Math.random() * Math.PI * 2;
      const phi = Math.acos(2 * Math.random() - 1);
      const r = 400 + Math.random() * 200;
      positions[i * 3] = r * Math.sin(phi) * Math.cos(theta);
      positions[i * 3 + 1] = r * Math.sin(phi) * Math.sin(theta);
      positions[i * 3 + 2] = r * Math.cos(phi);
    }
    geo.setAttribute("position", new THREE.BufferAttribute(positions, 3));
    return geo;
  }, [count]);

  return (
    <points geometry={geometry}>
      <pointsMaterial
        color="#ffffff"
        size={0.3}
        sizeAttenuation
        transparent
        opacity={0.85}
        depthWrite={false}
      />
    </points>
  );
}

/* ---------- Earth ------------------------------------------------ */
function Earth() {
  const meshRef = useRef<THREE.Mesh>(null!);
  const glowRef = useRef<THREE.Mesh>(null!);

  useFrame((_state, delta) => {
    if (meshRef.current) meshRef.current.rotation.y += delta * 0.05;
  });

  return (
    <group>
      {/* Main body */}
      <mesh ref={meshRef}>
        <sphereGeometry args={[EARTH_RADIUS, 64, 64]} />
        <meshPhongMaterial
          color="#1a6fbf"
          emissive="#0a2e5c"
          emissiveIntensity={0.35}
          shininess={25}
        />
      </mesh>

      {/* Atmosphere glow — inner */}
      <mesh ref={glowRef}>
        <sphereGeometry args={[EARTH_RADIUS * 1.08, 48, 48]} />
        <meshBasicMaterial
          color="#4da6ff"
          transparent
          opacity={0.12}
          side={THREE.BackSide}
          depthWrite={false}
          blending={THREE.AdditiveBlending}
        />
      </mesh>

      {/* Atmosphere glow — outer haze */}
      <mesh>
        <sphereGeometry args={[EARTH_RADIUS * 1.25, 48, 48]} />
        <meshBasicMaterial
          color="#3b8eed"
          transparent
          opacity={0.05}
          side={THREE.BackSide}
          depthWrite={false}
          blending={THREE.AdditiveBlending}
        />
      </mesh>

      {/* Label */}
      <Html position={[0, EARTH_RADIUS * 2.2, 0]} center distanceFactor={40}>
        <span
          style={{
            color: "#8cb4d8",
            fontSize: 11,
            fontFamily: "Inter, system-ui, sans-serif",
            fontWeight: 600,
            letterSpacing: 1.2,
            textTransform: "uppercase",
            userSelect: "none",
            whiteSpace: "nowrap",
            textShadow: "0 0 6px rgba(50,130,220,0.6)",
          }}
        >
          Earth
        </span>
      </Html>
    </group>
  );
}

/* ---------- Moon ------------------------------------------------- */
function Moon({
  position,
}: {
  position: [number, number, number];
}) {
  const meshRef = useRef<THREE.Mesh>(null!);

  useFrame((_s, delta) => {
    if (meshRef.current) meshRef.current.rotation.y += delta * 0.02;
  });

  return (
    <group position={position}>
      <mesh ref={meshRef}>
        <sphereGeometry args={[MOON_RADIUS, 48, 48]} />
        <meshPhongMaterial
          color="#b0b0b0"
          emissive="#444444"
          emissiveIntensity={0.15}
          shininess={5}
        />
      </mesh>

      {/* faint glow */}
      <mesh>
        <sphereGeometry args={[MOON_RADIUS * 1.15, 32, 32]} />
        <meshBasicMaterial
          color="#cccccc"
          transparent
          opacity={0.06}
          side={THREE.BackSide}
          depthWrite={false}
          blending={THREE.AdditiveBlending}
        />
      </mesh>

      <Html position={[0, MOON_RADIUS * 3, 0]} center distanceFactor={40}>
        <span
          style={{
            color: "#a0a0a0",
            fontSize: 11,
            fontFamily: "Inter, system-ui, sans-serif",
            fontWeight: 600,
            letterSpacing: 1.2,
            textTransform: "uppercase",
            userSelect: "none",
            whiteSpace: "nowrap",
            textShadow: "0 0 4px rgba(160,160,160,0.5)",
          }}
        >
          Moon
        </span>
      </Html>
    </group>
  );
}

/* ---------- Trajectory Path (smooth CatmullRom curve) ----------- */
function TrajectoryPath({
  points,
}: {
  points: Array<{ x_km: number; y_km: number; z_km: number }>;
}) {
  const { positions, colors } = useMemo(() => {
    if (points.length < 2) return { positions: [] as [number,number,number][], colors: [] as [number,number,number][] };

    // Convert raw data points to scene coordinates
    const rawPts = points.map(
      (p) => new THREE.Vector3(toScene(p.x_km), toScene(p.y_km), toScene(p.z_km))
    );

    // Create smooth CatmullRom spline through the points
    const curve = new THREE.CatmullRomCurve3(rawPts, false, "catmullrom", 0.5);

    // Sample many smooth points along the curve
    const numSamples = Math.min(rawPts.length * 4, 3000);
    const smoothPts = curve.getPoints(numSamples);

    const pos: [number, number, number][] = [];
    const col: [number, number, number][] = [];
    const startColor = new THREE.Color("#1e5bb8");
    const endColor = new THREE.Color("#FC3D21");
    const tmpColor = new THREE.Color();

    for (let i = 0; i < smoothPts.length; i++) {
      const p = smoothPts[i];
      pos.push([p.x, p.y, p.z]);
      const t = i / (smoothPts.length - 1);
      tmpColor.copy(startColor).lerp(endColor, t);
      col.push([tmpColor.r, tmpColor.g, tmpColor.b]);
    }

    return { positions: pos, colors: col };
  }, [points]);

  if (positions.length < 2) return null;

  return (
    <Line
      points={positions}
      vertexColors={colors}
      lineWidth={2.5}
      transparent
      opacity={0.9}
    />
  );
}

/* ---------- Orbital plane ring (faint reference) ---------------- */
function OrbitalPlaneRing() {
  const points = useMemo(() => {
    const pts: [number, number, number][] = [];
    for (let i = 0; i <= 128; i++) {
      const angle = (i / 128) * Math.PI * 2;
      pts.push([Math.cos(angle) * 19, 0, Math.sin(angle) * 19]);
    }
    return pts;
  }, []);

  return (
    <Line
      points={points}
      color="#1a3a5a"
      lineWidth={0.5}
      transparent
      opacity={0.15}
    />
  );
}

/* ---------- Orion spacecraft ------------------------------------- */
function OrionMarker({
  position,
  velocity,
}: {
  position: [number, number, number];
  velocity?: [number, number, number];
}) {
  const outerRef = useRef<THREE.Mesh>(null!);

  useFrame(({ clock }) => {
    if (!outerRef.current) return;
    const s = 1 + 0.3 * Math.sin(clock.getElapsedTime() * 3);
    outerRef.current.scale.setScalar(s);
  });

  // velocity arrow direction
  const arrowPoints = useMemo<[number, number, number][]>(() => {
    if (!velocity) return [];
    const dir = new THREE.Vector3(...velocity).normalize();
    const tip = dir.clone().multiplyScalar(1.8);
    return [
      [0, 0, 0],
      [tip.x, tip.y, tip.z],
    ];
  }, [velocity]);

  return (
    <group position={position}>
      {/* Core bright point */}
      <mesh>
        <sphereGeometry args={[0.18, 24, 24]} />
        <meshBasicMaterial color="#FC3D21" />
      </mesh>

      {/* Pulsing glow ring */}
      <mesh ref={outerRef}>
        <sphereGeometry args={[0.35, 24, 24]} />
        <meshBasicMaterial
          color="#FC3D21"
          transparent
          opacity={0.18}
          depthWrite={false}
          blending={THREE.AdditiveBlending}
        />
      </mesh>

      {/* Velocity arrow */}
      {arrowPoints.length === 2 && (
        <Line
          points={arrowPoints}
          color="#FC3D21"
          lineWidth={2}
          transparent
          opacity={0.8}
        />
      )}

      {/* Label */}
      <Html position={[0, 0.8, 0]} center distanceFactor={40}>
        <span
          style={{
            color: "#FC3D21",
            fontSize: 12,
            fontFamily: "Inter, system-ui, sans-serif",
            fontWeight: 700,
            letterSpacing: 1.5,
            textTransform: "uppercase",
            userSelect: "none",
            whiteSpace: "nowrap",
            textShadow: "0 0 8px rgba(252,61,33,0.7)",
          }}
        >
          Orion
        </span>
      </Html>
    </group>
  );
}

/* ---------- Distance lines --------------------------------------- */
function DistanceLines({
  orionPos,
  moonPos,
  distEarth,
  distMoon,
}: {
  orionPos: [number, number, number];
  moonPos: [number, number, number];
  distEarth?: number;
  distMoon?: number;
}) {
  const earthOrigin: [number, number, number] = [0, 0, 0];

  const fmt = (km: number) => {
    if (km >= 1_000_000) return `${(km / 1_000_000).toFixed(2)}M km`;
    return `${Math.round(km).toLocaleString()} km`;
  };

  const midEarth: [number, number, number] = [
    orionPos[0] / 2,
    orionPos[1] / 2,
    orionPos[2] / 2,
  ];
  const midMoon: [number, number, number] = [
    (orionPos[0] + moonPos[0]) / 2,
    (orionPos[1] + moonPos[1]) / 2,
    (orionPos[2] + moonPos[2]) / 2,
  ];

  return (
    <>
      {/* Earth -> Orion */}
      <Line
        points={[earthOrigin, orionPos]}
        color="#1e5bb8"
        lineWidth={0.8}
        transparent
        opacity={0.3}
        dashed
        dashSize={0.5}
        gapSize={0.3}
      />
      {distEarth != null && (
        <Html position={midEarth} center distanceFactor={60}>
          <span
            style={{
              color: "#6a9fd8",
              fontSize: 9,
              fontFamily: "Inter, system-ui, sans-serif",
              fontWeight: 500,
              userSelect: "none",
              whiteSpace: "nowrap",
              textShadow: "0 0 4px rgba(30,91,184,0.5)",
              background: "rgba(0,0,0,0.5)",
              padding: "1px 5px",
              borderRadius: 3,
            }}
          >
            {fmt(distEarth)}
          </span>
        </Html>
      )}

      {/* Moon -> Orion */}
      <Line
        points={[moonPos, orionPos]}
        color="#888888"
        lineWidth={0.8}
        transparent
        opacity={0.25}
        dashed
        dashSize={0.5}
        gapSize={0.3}
      />
      {distMoon != null && (
        <Html position={midMoon} center distanceFactor={60}>
          <span
            style={{
              color: "#999999",
              fontSize: 9,
              fontFamily: "Inter, system-ui, sans-serif",
              fontWeight: 500,
              userSelect: "none",
              whiteSpace: "nowrap",
              textShadow: "0 0 4px rgba(100,100,100,0.5)",
              background: "rgba(0,0,0,0.5)",
              padding: "1px 5px",
              borderRadius: 3,
            }}
          >
            {fmt(distMoon)}
          </span>
        </Html>
      )}
    </>
  );
}

/* ---------- Sun light (far away directional source) -------------- */
function SunLight() {
  return (
    <>
      <ambientLight intensity={0.08} />
      <pointLight
        position={[200, 100, 150]}
        intensity={2.5}
        color="#fff5e0"
        decay={0}
      />
      <pointLight
        position={[-80, -40, -60]}
        intensity={0.3}
        color="#3366aa"
        decay={0}
      />
    </>
  );
}

/* ================================================================== */
/*  Main exported component                                           */
/* ================================================================== */
const SpaceView: React.FC<SpaceViewProps> = ({
  trajectoryPoints,
  currentPosition,
  currentVelocity,
  moonPosition,
  distanceEarthKm,
  distanceMoonKm,
}) => {
  /* Moon scene position — use prop or default placeholder */
  const moonScenePos: [number, number, number] = useMemo(() => {
    if (moonPosition) {
      return [
        toScene(moonPosition.x_km),
        toScene(moonPosition.y_km),
        toScene(moonPosition.z_km),
      ];
    }
    return [19, 0, 0]; // approximate default
  }, [moonPosition]);

  /* Current Orion position in scene coordinates */
  const orionScenePos: [number, number, number] | null = useMemo(() => {
    if (!currentPosition) return null;
    return [
      toScene(currentPosition.x_km),
      toScene(currentPosition.y_km),
      toScene(currentPosition.z_km),
    ];
  }, [currentPosition]);

  /* Velocity vector in scene coordinates (direction only matters) */
  const velocityVec: [number, number, number] | undefined = useMemo(() => {
    if (!currentVelocity) return undefined;
    return [
      currentVelocity.vx_km_s,
      currentVelocity.vy_km_s,
      currentVelocity.vz_km_s,
    ];
  }, [currentVelocity]);

  /* Camera: frame Earth, Moon, and Orion — biased toward Orion */
  const { cameraPos, lookAt } = useMemo(() => {
    const orion = orionScenePos || [0, 0, 0] as [number, number, number];
    const moon = moonScenePos;

    // Scene center: weighted midpoint (30% toward Orion from scene center)
    const cx = (orion[0] + moon[0]) * 0.3;
    const cy = (orion[1] + moon[1]) * 0.3;
    const cz = (orion[2] + moon[2]) * 0.3;

    // Camera distance: enough to see all three, at ~1.1x the max extent
    const maxExtent = Math.max(
      Math.sqrt(orion[0] ** 2 + orion[1] ** 2 + orion[2] ** 2),
      Math.sqrt(moon[0] ** 2 + moon[1] ** 2 + moon[2] ** 2),
      10
    );
    const camDist = maxExtent * 1.1;

    return {
      cameraPos: [cx, camDist * 0.7, camDist * 0.5] as [number, number, number],
      lookAt: [cx, cy, cz] as [number, number, number],
    };
  }, [orionScenePos, moonScenePos]);

  return (
    <Canvas
      camera={{ position: cameraPos, fov: 45, near: 0.01, far: 2000 }}
      style={{ background: "#000005" }}
      gl={{
        antialias: true,
        alpha: false,
        powerPreference: "low-power" as const,
        stencil: false,
        depth: true,
        preserveDrawingBuffer: false
      } as any}
      dpr={[1, 1.5]}
    >
      {/* Lighting */}
      <SunLight />

      {/* Stars */}
      <Starfield count={2500} />

      {/* Orbital plane reference ring */}
      <OrbitalPlaneRing />

      {/* Earth at origin */}
      <Earth />

      {/* Moon */}
      <Moon position={moonScenePos} />

      {/* Smooth trajectory tube */}
      {trajectoryPoints.length > 1 && (
        <TrajectoryPath points={trajectoryPoints} />
      )}

      {/* Orion spacecraft */}
      {orionScenePos && (
        <OrionMarker position={orionScenePos} velocity={velocityVec} />
      )}

      {/* Distance reference lines */}
      {orionScenePos && (
        <DistanceLines
          orionPos={orionScenePos}
          moonPos={moonScenePos}
          distEarth={distanceEarthKm}
          distMoon={distanceMoonKm}
        />
      )}

      {/* Camera controls — target the scene center */}
      <OrbitControls
        enableDamping
        dampingFactor={0.08}
        target={lookAt}
        minDistance={1}
        maxDistance={300}
        enablePan
        zoomSpeed={0.8}
        rotateSpeed={0.5}
      />
    </Canvas>
  );
};

export default SpaceView;
