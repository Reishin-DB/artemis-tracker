import React, { useMemo, useState } from "react";

interface DigitalTwinProps {
  velocityVector: { vx: number; vy: number; vz: number };
  position: { x: number; y: number; z: number };
  phase: string;
  distanceEarthKm: number;
  distanceMoonKm: number;
  missionElapsedDisplay: string;
}

const SPEED_OF_LIGHT_KMS = 299792.458;

interface ComponentInfo {
  id: string;
  name: string;
  subsystem: string;
  description: string;
  specs: string[];
  color: string;
}

const COMPONENTS: Record<string, ComponentInfo> = {
  cm: {
    id: "cm", name: "Crew Module (CM)", subsystem: "Structures", color: "#e2e8f0",
    description: "Pressurized capsule housing the 4-person crew. Conical shape with AVCOAT ablative heat shield for atmospheric re-entry at 25,000 mph. Derived from Apollo but 50% larger by volume.",
    specs: ["Diameter: 5.02 m (16.5 ft)", "Habitable volume: 8.95 m³", "Mass: 10,400 kg", "Heat shield: AVCOAT ablative, rated 2,760°C", "Crew: 4 astronauts", "Life support: 21-day capability"],
  },
  windows: {
    id: "windows", name: "Crew Windows", subsystem: "Structures", color: "#4da6ff",
    description: "Four docking/rendezvous windows and two side observation windows. Triple-pane fused silica and borosilicate glass rated for vacuum, thermal cycling, and micrometeorite impact.",
    specs: ["6 total windows (4 docking, 2 side)", "Triple-pane construction", "Outer: fused silica", "Inner: acrylic pressure seal"],
  },
  heatshield: {
    id: "heatshield", name: "Heat Shield (TPS)", subsystem: "Thermal Protection", color: "#f59e0b",
    description: "Largest ablative heat shield ever built. AVCOAT material ablates during re-entry, carrying heat away. Protects crew during 25,000 mph entry — fastest human re-entry ever.",
    specs: ["Diameter: 5.02 m (largest ever flown)", "Material: AVCOAT epoxy novolac", "Re-entry temp: ~2,760°C (5,000°F)", "Re-entry speed: 40,000 km/h", "Skip re-entry for precision landing"],
  },
  docking: {
    id: "docking", name: "Docking System (NDS)", subsystem: "Mechanisms", color: "#94a3b8",
    description: "NASA Docking System at the apex. Compatible with International Docking System Standard for future Gateway station rendezvous. Not used on Artemis II (free-return mission).",
    specs: ["Type: NASA Docking System", "Standard: IDSS compliant", "Location: Forward apex of CM", "Not used on Artemis II"],
  },
  sm: {
    id: "sm", name: "European Service Module (ESM)", subsystem: "Propulsion & Power", color: "#94a3b8",
    description: "Built by Airbus for ESA. Provides propulsion, power, thermal control, and consumables. Houses main engine, RCS thrusters, and propellant tanks. Jettisoned before re-entry.",
    specs: ["Length: 4.5 m | Diameter: 5.2 m", "Mass: 15,461 kg (fueled)", "Propellant: 8,600 kg MMH/MON-3", "8 auxiliary thrusters (490 N each)", "24 RCS thrusters (220 N each)", "Builder: Airbus Defence & Space"],
  },
  engine: {
    id: "engine", name: "OMS Engine (AJ10-190)", subsystem: "Propulsion", color: "#94a3b8",
    description: "Aerojet Rocketdyne AJ10-190 derived from Space Shuttle OMS. Provides major delta-v burns including trajectory corrections. Hypergolic propellant for reliable ignition in deep space.",
    specs: ["Thrust: 26.7 kN (6,000 lbf)", "Isp: 316 s (vacuum)", "Propellant: MMH / MON-3 (hypergolic)", "Heritage: Space Shuttle OMS", "Burns: corrections, abort capability"],
  },
  solar: {
    id: "solar", name: "Solar Array Wings", subsystem: "Electrical Power", color: "#4da6ff",
    description: "Four solar array wings providing all electrical power. Triple-junction gallium arsenide cells. Arrays track the Sun and can be feathered edge-on during maneuvers.",
    specs: ["4 wings, 3 panels each", "Total power: 11.1 kW (EOL)", "Cell type: Triple-junction GaAs", "Wing span: 19 m tip-to-tip", "Panel area: ~62 m² total", "Heritage: ATV/Rosetta adapted"],
  },
  radiators: {
    id: "radiators", name: "Thermal Radiator Panels", subsystem: "Thermal Control", color: "#64748b",
    description: "Body-mounted radiators on the service module reject excess heat to space. Combined with internal fluid loops to maintain crew cabin at 21°C and electronics within operating range.",
    specs: ["Type: Body-mounted radiators", "Fluid: single-phase propylene glycol", "Cabin temp: 18-27°C maintained", "Rejects up to 7.5 kW thermal"],
  },
  comm: {
    id: "comm", name: "Communication Systems", subsystem: "Communications", color: "#4da6ff",
    description: "S-band phased array for voice/telemetry. Ka-band high-gain for data. Optical communications demo for laser comm. Tracked via NASA Deep Space Network.",
    specs: ["S-band: voice, telemetry, commands", "Ka-band: high-rate science data", "Optical: laser comm demo", "DSN: Goldstone, Canberra, Madrid", "Signal delay at Moon: ~1.3s one-way"],
  },
};

const DigitalTwin: React.FC<DigitalTwinProps> = ({
  velocityVector, position, phase, distanceEarthKm,
}) => {
  const [selected, setSelected] = useState<string | null>(null);

  const derived = useMemo(() => {
    const velAngleRad = Math.atan2(velocityVector.vz, velocityVector.vx);
    const velAngleDeg = (velAngleRad * 180) / Math.PI;
    const earthAngleDeg = (Math.atan2(-position.z, -position.x) * 180) / Math.PI;
    const maneuverPhases = ["tli", "trans-lunar", "departure", "correction"];
    const isManeuver = maneuverPhases.some((p) => phase.toLowerCase().includes(p));
    return { velAngleDeg, earthAngleDeg, isManeuver, lightDelaySec: distanceEarthKm / SPEED_OF_LIGHT_KMS };
  }, [velocityVector, position, phase, distanceEarthKm]);

  const info = selected ? COMPONENTS[selected] : null;
  const sel = (id: string) => (e: React.MouseEvent) => { e.stopPropagation(); setSelected(id); };
  const isActive = (id: string) => selected === id;

  // Non-rotated SVG — spacecraft always points up for clarity
  return (
    <div className="dt-container" onClick={() => setSelected(null)}>
      <div className="dt-layout">
        {/* SVG — detailed Orion MPCV, not rotated */}
        <svg viewBox="0 0 480 360" className="dt-svg" xmlns="http://www.w3.org/2000/svg">
          <defs>
            <radialGradient id="tg" cx="50%" cy="0%" r="80%"><stop offset="0%" stopColor="#FC3D21" stopOpacity="0.9"/><stop offset="100%" stopColor="#FC3D21" stopOpacity="0"/></radialGradient>
            <linearGradient id="cmGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#2a3a50"/><stop offset="100%" stopColor="#1a2535"/></linearGradient>
            <linearGradient id="smGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#1a2535"/><stop offset="100%" stopColor="#131d2a"/></linearGradient>
          </defs>

          {/* Grid */}
          <g opacity="0.03">
            {Array.from({length:15}).map((_,i)=><line key={`h${i}`} x1="0" y1={i*25} x2="480" y2={i*25} stroke="#4da6ff" strokeWidth="0.5"/>)}
            {Array.from({length:20}).map((_,i)=><line key={`v${i}`} x1={i*25} y1="0" x2={i*25} y2="360" stroke="#4da6ff" strokeWidth="0.5"/>)}
          </g>

          {/* ═══ SPACECRAFT centered at (240, 160) ═══ */}
          <g transform="translate(240, 160)">

            {/* Thruster exhaust */}
            {derived.isManeuver && <ellipse cx="0" cy="95" rx="14" ry="35" fill="url(#tg)"><animate attributeName="ry" values="28;38;28" dur="0.4s" repeatCount="indefinite"/></ellipse>}

            {/* ── ENGINE NOZZLE ── */}
            <g onClick={sel("engine")} style={{cursor:"pointer"}} pointerEvents="all">
              <rect x="-20" y="72" width="40" height="25" fill="transparent"/>
              <path d="M-14 78 L-18 95 L18 95 L14 78" fill="none" stroke={isActive("engine")?"#FC3D21":"#7a8a9a"} strokeWidth="1.5"/>
              <ellipse cx="0" cy="95" rx="18" ry="4" fill="none" stroke={isActive("engine")?"#FC3D21":"#5a6a7a"} strokeWidth="1"/>
              <path d="M-8 80 L-10 90 L10 90 L8 80" fill="none" stroke="#3a4a5a" strokeWidth="0.5"/>
              <text x="25" y="90" fill={isActive("engine")?"#FC3D21":"#5a6a7a"} fontSize="7" fontFamily="'JetBrains Mono',monospace">AJ10-190</text>
            </g>

            {/* ── SERVICE MODULE ── */}
            <g onClick={sel("sm")} style={{cursor:"pointer"}} pointerEvents="all">
              <rect x="-30" y="10" width="60" height="68" fill="transparent"/>
              <rect x="-24" y="14" width="48" height="64" rx="3" fill="url(#smGrad)" stroke={isActive("sm")?"#FC3D21":"#5a6a7a"} strokeWidth={isActive("sm")?2:1.2}/>
              {/* Panel lines */}
              {[24,34,44,54,64].map(y=><line key={y} x1="-24" y1={y} x2="24" y2={y} stroke="#2a3a4a" strokeWidth="0.5"/>)}
              {/* Vent ports */}
              <circle cx="-20" cy="40" r="2" fill="none" stroke="#3a4a5a" strokeWidth="0.5"/>
              <circle cx="20" cy="40" r="2" fill="none" stroke="#3a4a5a" strokeWidth="0.5"/>
              <circle cx="-20" cy="60" r="2" fill="none" stroke="#3a4a5a" strokeWidth="0.5"/>
              <circle cx="20" cy="60" r="2" fill="none" stroke="#3a4a5a" strokeWidth="0.5"/>
              {/* RCS thruster quads */}
              <rect x="-28" y="20" width="4" height="8" rx="1" fill="none" stroke="#4a5a6a" strokeWidth="0.7"/>
              <rect x="24" y="20" width="4" height="8" rx="1" fill="none" stroke="#4a5a6a" strokeWidth="0.7"/>
              <rect x="-28" y="55" width="4" height="8" rx="1" fill="none" stroke="#4a5a6a" strokeWidth="0.7"/>
              <rect x="24" y="55" width="4" height="8" rx="1" fill="none" stroke="#4a5a6a" strokeWidth="0.7"/>
              <text x="0" y="42" textAnchor="middle" fill="#4a5a6a" fontSize="7" fontFamily="'JetBrains Mono',monospace" fontWeight="600">ESM</text>
            </g>

            {/* ── RADIATOR PANELS (on SM body) ── */}
            <g onClick={sel("radiators")} style={{cursor:"pointer"}} pointerEvents="all">
              <rect x="-22" y="16" width="44" height="10" fill="transparent"/>
              <rect x="-22" y="16" width="20" height="6" rx="1" fill="none" stroke={isActive("radiators")?"#FC3D21":"#3a4a5a"} strokeWidth="0.8" strokeDasharray="2 1"/>
              <rect x="2" y="16" width="20" height="6" rx="1" fill="none" stroke={isActive("radiators")?"#FC3D21":"#3a4a5a"} strokeWidth="0.8" strokeDasharray="2 1"/>
            </g>

            {/* ── SM/CM SEPARATION RING ── */}
            <line x1="-26" y1="13" x2="26" y2="13" stroke="#3a5a6a" strokeWidth="2" strokeDasharray="3 2"/>

            {/* ── CREW MODULE ── */}
            <g onClick={sel("cm")} style={{cursor:"pointer"}} pointerEvents="all">
              <path d="M-30 13 L-30 10 L-18 -40 L18 -40 L30 10 L30 13 Z" fill="transparent"/>
              <path d="M-30 12 L-18 -38 L18 -38 L30 12 Z" fill="url(#cmGrad)" stroke={isActive("cm")?"#FC3D21":"#c0d0e0"} strokeWidth={isActive("cm")?2:1.5}/>
              {/* Capsule structure lines */}
              <line x1="-25" y1="0" x2="25" y2="0" stroke="#2a3a4a" strokeWidth="0.6"/>
              <line x1="-22" y1="-10" x2="22" y2="-10" stroke="#2a3a4a" strokeWidth="0.6"/>
              <line x1="-20" y1="-20" x2="20" y2="-20" stroke="#2a3a4a" strokeWidth="0.6"/>
              {/* Side panels */}
              <line x1="-10" y1="12" x2="-14" y2="-38" stroke="#2a3a4a" strokeWidth="0.4"/>
              <line x1="10" y1="12" x2="14" y2="-38" stroke="#2a3a4a" strokeWidth="0.4"/>
            </g>

            {/* ── WINDOWS ── */}
            <g onClick={sel("windows")} style={{cursor:"pointer"}} pointerEvents="all">
              <rect x="-16" y="-18" width="32" height="14" fill="transparent"/>
              {/* Main windows */}
              <rect x="-14" y="-14" width="6" height="5" rx="1" fill="rgba(77,166,255,0.1)" stroke={isActive("windows")?"#FC3D21":"#4da6ff"} strokeWidth={isActive("windows")?1.5:0.8}/>
              <rect x="-5" y="-16" width="10" height="5" rx="1" fill="rgba(77,166,255,0.1)" stroke={isActive("windows")?"#FC3D21":"#4da6ff"} strokeWidth={isActive("windows")?1.5:0.8}/>
              <rect x="8" y="-14" width="6" height="5" rx="1" fill="rgba(77,166,255,0.1)" stroke={isActive("windows")?"#FC3D21":"#4da6ff"} strokeWidth={isActive("windows")?1.5:0.8}/>
              {/* Side observation */}
              <rect x="-26" y="-2" width="5" height="4" rx="1" fill="rgba(77,166,255,0.05)" stroke={isActive("windows")?"#FC3D21":"#3a7abf"} strokeWidth="0.6"/>
              <rect x="21" y="-2" width="5" height="4" rx="1" fill="rgba(77,166,255,0.05)" stroke={isActive("windows")?"#FC3D21":"#3a7abf"} strokeWidth="0.6"/>
            </g>

            {/* ── HEAT SHIELD ── */}
            <g onClick={sel("heatshield")} style={{cursor:"pointer"}} pointerEvents="all">
              <rect x="-32" y="10" width="64" height="8" fill="transparent"/>
              <line x1="-30" y1="13" x2="30" y2="13" stroke={isActive("heatshield")?"#FC3D21":"#f59e0b"} strokeWidth="4" opacity={isActive("heatshield")?1:0.6}/>
              <line x1="-28" y1="15" x2="28" y2="15" stroke={isActive("heatshield")?"#FC3D21":"#b57a0b"} strokeWidth="1" opacity="0.4"/>
            </g>

            {/* ── DOCKING ADAPTER ── */}
            <g onClick={sel("docking")} style={{cursor:"pointer"}} pointerEvents="all">
              <rect x="-10" y="-56" width="20" height="20" fill="transparent"/>
              <rect x="-7" y="-48" width="14" height="10" rx="2" fill="none" stroke={isActive("docking")?"#FC3D21":"#7a8a9a"} strokeWidth={isActive("docking")?1.5:1}/>
              <circle cx="0" cy="-52" r="5" fill="none" stroke={isActive("docking")?"#FC3D21":"#5a6a7a"} strokeWidth="0.8"/>
              <line x1="-3" y1="-52" x2="3" y2="-52" stroke="#4a5a6a" strokeWidth="0.5"/>
              <line x1="0" y1="-55" x2="0" y2="-49" stroke="#4a5a6a" strokeWidth="0.5"/>
            </g>

            {/* ── SOLAR ARRAYS — LEFT ── */}
            <g onClick={sel("solar")} style={{cursor:"pointer"}}  pointerEvents="all">
              <rect x="-155" y="25" width="130" height="22" fill="transparent"/>
              <line x1="-24" y1="36" x2="-45" y2="36" stroke="#7a8a9a" strokeWidth="1.2"/>
              {/* Panel 1 */}
              <rect x="-95" y="27" width="50" height="18" rx="1" fill="rgba(77,166,255,0.05)" stroke={isActive("solar")?"#FC3D21":"#4da6ff"} strokeWidth={isActive("solar")?1.5:1}/>
              {[10,20,30,40].map(dx=><line key={dx} x1={-95+dx} y1="27" x2={-95+dx} y2="45" stroke="#2a5a8a" strokeWidth="0.3"/>)}
              <line x1="-95" y1="36" x2="-45" y2="36" stroke="#2a5a8a" strokeWidth="0.3"/>
              {/* Panel 2 */}
              <rect x="-150" y="27" width="50" height="18" rx="1" fill="rgba(77,166,255,0.05)" stroke={isActive("solar")?"#FC3D21":"#4da6ff"} strokeWidth={isActive("solar")?1.5:1}/>
              {[10,20,30,40].map(dx=><line key={dx} x1={-150+dx} y1="27" x2={-150+dx} y2="45" stroke="#2a5a8a" strokeWidth="0.3"/>)}
              <line x1="-150" y1="36" x2="-100" y2="36" stroke="#2a5a8a" strokeWidth="0.3"/>
              <line x1="-95" y1="36" x2="-100" y2="36" stroke="#7a8a9a" strokeWidth="0.8"/>
            </g>

            {/* ── SOLAR ARRAYS — RIGHT ── */}
            <g onClick={sel("solar")} style={{cursor:"pointer"}}  pointerEvents="all">
              <rect x="25" y="25" width="130" height="22" fill="transparent"/>
              <line x1="24" y1="36" x2="45" y2="36" stroke="#7a8a9a" strokeWidth="1.2"/>
              <rect x="45" y="27" width="50" height="18" rx="1" fill="rgba(77,166,255,0.05)" stroke={isActive("solar")?"#FC3D21":"#4da6ff"} strokeWidth={isActive("solar")?1.5:1}/>
              {[10,20,30,40].map(dx=><line key={dx} x1={45+dx} y1="27" x2={45+dx} y2="45" stroke="#2a5a8a" strokeWidth="0.3"/>)}
              <line x1="45" y1="36" x2="95" y2="36" stroke="#2a5a8a" strokeWidth="0.3"/>
              <rect x="100" y="27" width="50" height="18" rx="1" fill="rgba(77,166,255,0.05)" stroke={isActive("solar")?"#FC3D21":"#4da6ff"} strokeWidth={isActive("solar")?1.5:1}/>
              {[10,20,30,40].map(dx=><line key={dx} x1={100+dx} y1="27" x2={100+dx} y2="45" stroke="#2a5a8a" strokeWidth="0.3"/>)}
              <line x1="100" y1="36" x2="150" y2="36" stroke="#2a5a8a" strokeWidth="0.3"/>
              <line x1="95" y1="36" x2="100" y2="36" stroke="#7a8a9a" strokeWidth="0.8"/>
            </g>

            {/* ── COMM ANTENNAS ── */}
            <g onClick={sel("comm")} style={{cursor:"pointer"}} pointerEvents="all">
              <rect x="-36" y="-35" width="10" height="10" fill="transparent"/>
              {/* S-band phased array */}
              <rect x="-34" y="-32" width="6" height="6" rx="1" fill="none" stroke={isActive("comm")?"#FC3D21":"#4da6ff"} strokeWidth="0.8"/>
              <line x1="-31" y1="-32" x2="-31" y2="-26" stroke="#4da6ff" strokeWidth="0.3"/>
              <line x1="-34" y1="-29" x2="-28" y2="-29" stroke="#4da6ff" strokeWidth="0.3"/>
              {/* Ka-band high gain */}
              <circle cx="32" cy="-28" r="4" fill="none" stroke={isActive("comm")?"#FC3D21":"#4da6ff"} strokeWidth="0.8"/>
              <circle cx="32" cy="-28" r="1.5" fill="none" stroke="#3a7abf" strokeWidth="0.5"/>
              <rect x="26" y="-34" width="12" height="12" fill="transparent"/>
            </g>
          </g>

          {/* ── DATA TRANSMISSION LINES (outside spacecraft group) ── */}

          {/* Comm beam to Earth */}
          <g onClick={sel("comm")} style={{cursor:"pointer"}}>
            <line x1="240" y1="160" x2={240 + 160 * Math.cos((derived.earthAngleDeg * Math.PI) / 180)} y2={160 + 160 * Math.sin((derived.earthAngleDeg * Math.PI) / 180)} stroke="#4da6ff" strokeWidth="2" strokeDasharray="8 5" opacity="0.4">
              <animate attributeName="stroke-dashoffset" values="0;-26" dur="1.5s" repeatCount="indefinite"/>
            </line>
            <text x={240 + 140 * Math.cos((derived.earthAngleDeg * Math.PI) / 180)} y={160 + 140 * Math.sin((derived.earthAngleDeg * Math.PI) / 180) - 8} fill="#4da6ff" fontSize="8" fontFamily="'JetBrains Mono',monospace" textAnchor="middle" opacity="0.6">DSN LINK</text>
          </g>

          {/* Velocity vector arrow */}
          <line x1="240" y1="160" x2={240 + 80 * Math.cos((derived.velAngleDeg * Math.PI) / 180)} y2={160 + 80 * Math.sin((derived.velAngleDeg * Math.PI) / 180)} stroke="#FC3D21" strokeWidth="1.5" opacity="0.6"/>
          <text x={240 + 90 * Math.cos((derived.velAngleDeg * Math.PI) / 180)} y={160 + 90 * Math.sin((derived.velAngleDeg * Math.PI) / 180)} fill="#FC3D21" fontSize="7" fontFamily="'JetBrains Mono',monospace" fontWeight="600" textAnchor="middle" opacity="0.6">VEL</text>

          {/* Earth direction */}
          <line x1="240" y1="160" x2={240 + 95 * Math.cos((derived.earthAngleDeg * Math.PI) / 180)} y2={160 + 95 * Math.sin((derived.earthAngleDeg * Math.PI) / 180)} stroke="#0B3D91" strokeWidth="1.5" strokeDasharray="4 3" opacity="0.4"/>
          <text x={240 + 108 * Math.cos((derived.earthAngleDeg * Math.PI) / 180)} y={160 + 108 * Math.sin((derived.earthAngleDeg * Math.PI) / 180)} fill="#4da6ff" fontSize="7" fontFamily="'JetBrains Mono',monospace" fontWeight="600" textAnchor="middle" opacity="0.5">EARTH</text>

          {/* ── ANNOTATIONS ── */}

          {/* Dimension lines */}
          <g opacity="0.3">
            <line x1="200" y1="100" x2="200" y2="270" stroke="#3a5a7a" strokeWidth="0.5" strokeDasharray="2 2"/>
            <line x1="280" y1="100" x2="280" y2="270" stroke="#3a5a7a" strokeWidth="0.5" strokeDasharray="2 2"/>
            <text x="240" y="280" textAnchor="middle" fill="#3a5a7a" fontSize="7" fontFamily="'JetBrains Mono',monospace">5.02 m</text>
          </g>

          {/* Light delay */}
          <text x="460" y="18" textAnchor="end" fill="#64748b" fontSize="8" fontFamily="'JetBrains Mono',monospace">LIGHT DELAY</text>
          <text x="460" y="34" textAnchor="end" fill="#f59e0b" fontSize="13" fontFamily="'JetBrains Mono',monospace" fontWeight="700">{derived.lightDelaySec.toFixed(2)}s</text>

          {/* Phase */}
          <text x="20" y="18" fill="#64748b" fontSize="8" fontFamily="'JetBrains Mono',monospace">PHASE</text>
          <text x="20" y="34" fill="#10b981" fontSize="11" fontFamily="'JetBrains Mono',monospace" fontWeight="700">{phase.toUpperCase().replace(/_/g," ") || "ACQUIRING"}</text>

        </svg>

        {/* ── Info overlay — right side of SVG ── */}
        {info && (
          <div className="dt-info-overlay">
            <div className="dt-info-header">
              <span className="dt-info-dot" style={{background: info.color}}/>
              <span className="dt-info-name">{info.name}</span>
              <button className="dt-info-close" onClick={() => setSelected(null)}>×</button>
            </div>
            <span className="dt-info-subsystem">{info.subsystem}</span>
            <p className="dt-info-desc">{info.description}</p>
            <ul className="dt-info-specs">
              {info.specs.map((s, i) => <li key={i}>{s}</li>)}
            </ul>
          </div>
        )}

        {/* ── NASA Live PiP ── */}
        <div className="dt-pip">
          <div className="dt-pip-header">
            <span className="dt-pip-dot" />
            NASA LIVE
          </div>
          <iframe
            src="https://www.youtube.com/embed/m3kR2KK8TEs?autoplay=1&mute=1&controls=1&modestbranding=1&playsinline=1&rel=0"
            title="NASA Live"
            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
            allowFullScreen
            sandbox="allow-scripts allow-same-origin allow-popups allow-presentation"
            referrerPolicy="no-referrer-when-downgrade"
            className="dt-pip-video"
          />
        </div>
      </div>
    </div>
  );
};

export default DigitalTwin;
