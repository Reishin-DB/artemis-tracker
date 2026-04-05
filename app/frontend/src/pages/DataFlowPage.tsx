import { Link } from "react-router-dom";

const SVG_CONTENT = `
<svg viewBox="0 0 960 640" xmlns="http://www.w3.org/2000/svg" style="width:100%;height:auto;">
  <style>
    text { font-family: 'Inter', system-ui, sans-serif; fill: #e2e8f0; }
    .title { font-size: 12px; font-weight: 700; }
    .subtitle { font-size: 9px; fill: #94a3b8; }
    .label { font-size: 9px; fill: #94a3b8; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
    .box { rx: 8; }
    .edge { stroke-width: 2; fill: none; }
    .edge-animated { stroke-dasharray: 8 4; animation: fd 1.5s linear infinite; }
    .edge-track { stroke: rgba(255,255,255,0.04); stroke-width: 2; fill: none; }
    @keyframes fd { to { stroke-dashoffset: -24; } }
  </style>

  <rect width="960" height="640" fill="#0a0e1a" rx="12"/>

  <!-- ═══ ROW 1: DATA SOURCES ═══ -->
  <text x="20" y="28" class="label">REAL-TIME NASA DATA SOURCES</text>

  <rect x="30" y="40" width="200" height="56" class="box" fill="#1e293b" stroke="#10b981" stroke-width="1.5"/>
  <text x="130" y="62" text-anchor="middle" class="title" style="fill:#10b981">🛰 JPL Horizons API</text>
  <text x="130" y="78" text-anchor="middle" class="subtitle">Orion vectors (COMMAND=-1024)</text>
  <text x="130" y="90" text-anchor="middle" class="subtitle" style="fill:#10b981;font-size:8px">Every 5 min | JSON/CSV</text>

  <rect x="260" y="40" width="200" height="56" class="box" fill="#1e293b" stroke="#3b82f6" stroke-width="1.5"/>
  <text x="360" y="62" text-anchor="middle" class="title" style="fill:#3b82f6">📡 AROW Ephemeris</text>
  <text x="360" y="78" text-anchor="middle" class="subtitle">CCSDS OEM state vectors</text>
  <text x="360" y="90" text-anchor="middle" class="subtitle" style="fill:#3b82f6;font-size:8px">Periodic | J2000 4-min intervals</text>

  <rect x="490" y="40" width="200" height="56" class="box" fill="#1e293b" stroke="#f59e0b" stroke-width="1.5"/>
  <text x="590" y="62" text-anchor="middle" class="title" style="fill:#f59e0b">📷 NASA Image API</text>
  <text x="590" y="78" text-anchor="middle" class="subtitle">Artemis II media catalog</text>
  <text x="590" y="90" text-anchor="middle" class="subtitle" style="fill:#f59e0b;font-size:8px">Every 15 min | REST JSON</text>

  <rect x="720" y="40" width="210" height="56" class="box" fill="#1e293b" stroke="#8b5cf6" stroke-width="1.5"/>
  <text x="825" y="62" text-anchor="middle" class="title" style="fill:#8b5cf6">📻 DSN Now + NASA Blog</text>
  <text x="825" y="78" text-anchor="middle" class="subtitle">Antenna status + mission updates</text>
  <text x="825" y="90" text-anchor="middle" class="subtitle" style="fill:#8b5cf6;font-size:8px">30 sec / 10 min | XML / RSS</text>

  <!-- ═══ Source → Bronze Arrows ═══ -->
  <path d="M130,96 L130,155" class="edge-track"/><path d="M130,96 L130,155" class="edge edge-animated" stroke="#10b981"/>
  <path d="M360,96 L310,155" class="edge-track"/><path d="M360,96 L310,155" class="edge edge-animated" stroke="#3b82f6"/>
  <path d="M590,96 L690,155" class="edge-track"/><path d="M590,96 L690,155" class="edge edge-animated" stroke="#f59e0b"/>
  <path d="M825,96 L860,155" class="edge-track"/><path d="M825,96 L860,155" class="edge edge-animated" stroke="#8b5cf6"/>

  <!-- ═══ ROW 2: DATABRICKS LAKEHOUSE ═══ -->
  <text x="20" y="145" class="label">DATABRICKS LAKEHOUSE (UNITY CATALOG)</text>

  <!-- Bronze -->
  <rect x="30" y="158" width="210" height="80" class="box" fill="#0f172a" stroke="#10b981" stroke-width="1.5"/>
  <text x="135" y="178" text-anchor="middle" class="title" style="fill:#10b981">🥉 BRONZE</text>
  <text x="135" y="192" text-anchor="middle" class="subtitle">Raw append-only ingestion</text>
  <text x="48" y="208" class="subtitle" style="font-size:8px;fill:#64748b">• raw_horizons_vectors</text>
  <text x="48" y="220" class="subtitle" style="font-size:8px;fill:#64748b">• raw_arow_ephemeris</text>
  <text x="48" y="232" class="subtitle" style="font-size:8px;fill:#64748b">• raw_nasa_media</text>

  <!-- Silver -->
  <rect x="270" y="158" width="210" height="80" class="box" fill="#0f172a" stroke="#3b82f6" stroke-width="1.5"/>
  <text x="375" y="178" text-anchor="middle" class="title" style="fill:#3b82f6">🥈 SILVER</text>
  <text x="375" y="192" text-anchor="middle" class="subtitle">Normalized, typed, deduped</text>
  <text x="288" y="208" class="subtitle" style="font-size:8px;fill:#64748b">• telemetry_normalized</text>
  <text x="288" y="220" class="subtitle" style="font-size:8px;fill:#64748b">• mission_events</text>
  <text x="288" y="232" class="subtitle" style="font-size:8px;fill:#64748b">• data_quality_log</text>

  <!-- Gold -->
  <rect x="510" y="158" width="210" height="80" class="box" fill="#0f172a" stroke="#f59e0b" stroke-width="1.5"/>
  <text x="615" y="178" text-anchor="middle" class="title" style="fill:#f59e0b">🥇 GOLD</text>
  <text x="615" y="192" text-anchor="middle" class="subtitle">Business-ready views</text>
  <text x="528" y="208" class="subtitle" style="font-size:8px;fill:#64748b">• current_status</text>
  <text x="528" y="220" class="subtitle" style="font-size:8px;fill:#64748b">• trajectory_history</text>
  <text x="528" y="232" class="subtitle" style="font-size:8px;fill:#64748b">• milestones / diagnostics</text>

  <!-- Lakebase -->
  <rect x="750" y="158" width="180" height="80" class="box" fill="#0f172a" stroke="#FC3D21" stroke-width="1.5"/>
  <text x="840" y="178" text-anchor="middle" class="title" style="fill:#FC3D21">🐘 LAKEBASE</text>
  <text x="840" y="192" text-anchor="middle" class="subtitle">Managed Postgres serving</text>
  <text x="768" y="208" class="subtitle" style="font-size:8px;fill:#64748b">• Sub-ms API reads</text>
  <text x="768" y="220" class="subtitle" style="font-size:8px;fill:#64748b">• 1,121 trajectory points</text>
  <text x="768" y="232" class="subtitle" style="font-size:8px;fill:#64748b">• 9 milestones</text>

  <!-- Medallion flow arrows -->
  <path d="M240,198 L270,198" class="edge-track"/><path d="M240,198 L270,198" class="edge edge-animated" stroke="#3b82f6"/>
  <path d="M480,198 L510,198" class="edge-track"/><path d="M480,198 L510,198" class="edge edge-animated" stroke="#f59e0b"/>
  <path d="M720,198 L750,198" class="edge-track"/><path d="M720,198 L750,198" class="edge edge-animated" stroke="#FC3D21"/>

  <!-- ═══ UC Governance Box ═══ -->
  <rect x="20" y="255" width="920" height="55" rx="8" fill="none" stroke="#FC3D21" stroke-width="2" stroke-dasharray="6 3" opacity="0.6"/>
  <rect x="300" y="248" width="360" height="18" rx="5" fill="#0a0e1a"/>
  <text x="480" y="262" text-anchor="middle" style="font-size:11px;font-weight:700;fill:#FC3D21;">🛡️  UNITY CATALOG GOVERNANCE + DATABRICKS WORKFLOWS</text>

  <text x="80" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">Service Principals</text>
  <text x="240" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">GRANT / DENY</text>
  <text x="400" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">Data Quality Monitors</text>
  <text x="570" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">Scheduled Jobs (5 min)</text>
  <text x="740" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">SQL Alerts</text>
  <text x="880" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">Lineage</text>

  <!-- ═══ ROW 3: SERVING LAYER ═══ -->
  <text x="20" y="335" class="label">FASTAPI BACKEND (DATABRICKS APP)</text>

  <rect x="80" y="348" width="160" height="48" class="box" fill="#1e293b" stroke="#10b981" stroke-width="1"/>
  <text x="160" y="370" text-anchor="middle" class="title">/api/v1/current</text>
  <text x="160" y="384" text-anchor="middle" class="subtitle">30s cache | live position</text>

  <rect x="260" y="348" width="160" height="48" class="box" fill="#1e293b" stroke="#3b82f6" stroke-width="1"/>
  <text x="340" y="370" text-anchor="middle" class="title">/api/v1/path</text>
  <text x="340" y="384" text-anchor="middle" class="subtitle">2m cache | full trajectory</text>

  <rect x="440" y="348" width="160" height="48" class="box" fill="#1e293b" stroke="#f59e0b" stroke-width="1"/>
  <text x="520" y="370" text-anchor="middle" class="title">/api/v1/milestones</text>
  <text x="520" y="384" text-anchor="middle" class="subtitle">5m cache | timeline</text>

  <rect x="620" y="348" width="140" height="48" class="box" fill="#1e293b" stroke="#8b5cf6" stroke-width="1"/>
  <text x="690" y="370" text-anchor="middle" class="title">/api/v1/media</text>
  <text x="690" y="384" text-anchor="middle" class="subtitle">10m cache | images</text>

  <rect x="780" y="348" width="150" height="48" class="box" fill="#1e293b" stroke="#FC3D21" stroke-width="1"/>
  <text x="855" y="370" text-anchor="middle" class="title">/api/v1/diagnostics</text>
  <text x="855" y="384" text-anchor="middle" class="subtitle">30s cache | ops health</text>

  <!-- Gold/Lakebase → API arrows -->
  <path d="M840,238 L840,348" class="edge-track"/><path d="M840,238 L855,348" class="edge edge-animated" stroke="#FC3D21"/>
  <path d="M615,238 L520,348" class="edge-track"/><path d="M615,238 L520,348" class="edge edge-animated" stroke="#f59e0b"/>
  <path d="M615,238 L340,348" class="edge-track"/><path d="M615,238 L340,348" class="edge edge-animated" stroke="#3b82f6"/>
  <path d="M615,238 L160,348" class="edge-track"/><path d="M615,238 L160,348" class="edge edge-animated" stroke="#10b981"/>

  <!-- ═══ ROW 4: FRONTEND VIEWS ═══ -->
  <text x="20" y="425" class="label">REACT FRONTEND (3 VIEWS)</text>

  <rect x="80" y="438" width="240" height="70" class="box" fill="#0a1a12" stroke="#10b981" stroke-width="1.5"/>
  <text x="200" y="462" text-anchor="middle" style="font-size:22px;">🌍</text>
  <text x="200" y="480" text-anchor="middle" class="title" style="fill:#10b981">MISSION TRACKER</text>
  <text x="200" y="494" text-anchor="middle" class="subtitle">3D orbit | stats | timeline | media</text>

  <rect x="360" y="438" width="240" height="70" class="box" fill="#1a0f0a" stroke="#f59e0b" stroke-width="1.5"/>
  <text x="480" y="462" text-anchor="middle" style="font-size:22px;">🛸</text>
  <text x="480" y="480" text-anchor="middle" class="title" style="fill:#f59e0b">COMMAND CENTER</text>
  <text x="480" y="494" text-anchor="middle" class="subtitle">Digital twin | DSN | crew | telemetry</text>

  <rect x="640" y="438" width="240" height="70" class="box" fill="#10091a" stroke="#8b5cf6" stroke-width="1.5"/>
  <text x="760" y="462" text-anchor="middle" style="font-size:22px;">📊</text>
  <text x="760" y="480" text-anchor="middle" class="title" style="fill:#8b5cf6">ADMIN DIAGNOSTICS</text>
  <text x="760" y="494" text-anchor="middle" class="subtitle">Source health | errors | alerts | jobs</text>

  <!-- API → Frontend arrows -->
  <path d="M160,396 L200,438" class="edge-track"/><path d="M160,396 L200,438" class="edge edge-animated" stroke="#10b981"/>
  <path d="M520,396 L480,438" class="edge-track"/><path d="M520,396 L480,438" class="edge edge-animated" stroke="#f59e0b"/>
  <path d="M855,396 L760,438" class="edge-track"/><path d="M855,396 L760,438" class="edge edge-animated" stroke="#8b5cf6"/>

  <!-- ═══ ROW 5: FALLBACK + LIVE DATA ═══ -->
  <text x="20" y="540" class="label">RESILIENCE: HORIZONS LIVE FALLBACK</text>

  <rect x="80" y="553" width="800" height="40" class="box" fill="#0f172a" stroke="#10b981" stroke-width="1" stroke-dasharray="4 2"/>
  <text x="480" y="575" text-anchor="middle" class="title" style="fill:#10b981">🔄 If Lakebase is unreachable → FastAPI queries JPL Horizons directly → Real-time data always available</text>
  <text x="480" y="588" text-anchor="middle" class="subtitle">No simulated data. No mocks. Only real NASA mission telemetry. Always.</text>

  <!-- Footer -->
  <text x="480" y="625" text-anchor="middle" style="font-size:10px;fill:#475569;">
    Artemis II Mission Tracker — Built on Databricks | Lakebase | Unity Catalog | JPL Horizons | NASA AROW
  </text>
</svg>`;

const HOW_IT_WORKS = [
  {
    icon: "🛰",
    title: "Real NASA Data Sources",
    text: "JPL Horizons API provides Orion state vectors (COMMAND=-1024) every 5 minutes. AROW publishes CCSDS OEM ephemeris files with 4-minute interval position/velocity data in J2000 Earth-centered frame. NASA Image API supplies mission photography.",
    color: "#10b981",
  },
  {
    icon: "📐",
    title: "Medallion Lakehouse",
    text: "Bronze captures raw API responses and file payloads with checksums. Silver normalizes coordinates, computes derived fields (distance, speed, lat/lon), deduplicates, and logs data quality. Gold serves business-ready views for the API layer.",
    color: "#3b82f6",
  },
  {
    icon: "🐘",
    title: "Lakebase Serving",
    text: "Gold data syncs to Lakebase (managed Postgres) for sub-millisecond API reads. 1,121 trajectory points from launch to now. The FastAPI backend queries Lakebase with TTL caching at each endpoint.",
    color: "#FC3D21",
  },
  {
    icon: "🛡️",
    title: "Governance & Observability",
    text: "Unity Catalog provides lineage, access control, and data profiling. Databricks Workflows orchestrate ingestion on schedule. SQL alerts fire on stale feeds or parse failures. Every raw payload is archived for replay.",
    color: "#f59e0b",
  },
  {
    icon: "🔄",
    title: "Resilient Fallback",
    text: "If Lakebase or the SQL warehouse is unreachable, the backend falls back to querying JPL Horizons directly. The app always shows real live Orion position data - never simulated, never stale without warning.",
    color: "#8b5cf6",
  },
  {
    icon: "🌍",
    title: "3D Visualization",
    text: "React Three Fiber renders Earth, Moon, full trajectory path, and Orion's current position in a rotatable 3D scene. Distance lines, color-graded path, pulsing spacecraft marker, and 4,000 stars. All coordinates from real J2000 ECI vectors.",
    color: "#10b981",
  },
];

export default function DataFlowPage() {
  return (
    <div className="app">
      <nav className="topbar">
        <div className="topbar-left">
          <span className="topbar-title">ARTEMIS II</span>
          <span className="topbar-subtitle">Data Flow</span>
        </div>
        <div className="topbar-right">
          <div className="live-indicator"><span className="live-dot" />Live</div>
          <Link to="/" className="topbar-nav-link">Tracker</Link>
          <Link to="/command-center" className="topbar-nav-link">Command Center</Link>
          <Link to="/data-flow" className="topbar-nav-link active">Data Flow</Link>
          <Link to="/admin" className="topbar-nav-link">Admin</Link>
        </div>
      </nav>

      <main className="main-content" style={{ padding: "16px 20px" }}>
        <div
          dangerouslySetInnerHTML={{ __html: SVG_CONTENT }}
          style={{ borderRadius: 12, overflow: "hidden", border: "1px solid #1e293b", maxWidth: 960, margin: "0 auto" }}
        />

        <div style={{ maxWidth: 960, margin: "24px auto 0" }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: "#94a3b8", letterSpacing: 1, textTransform: "uppercase", marginBottom: 12 }}>
            HOW IT WORKS
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
            {HOW_IT_WORKS.map((card) => (
              <div key={card.title} style={{
                background: "#111827", border: "1px solid #1e293b", borderRadius: 8, padding: 14,
                borderLeft: `3px solid ${card.color}`,
              }}>
                <div style={{ fontSize: 20, marginBottom: 6 }}>{card.icon}</div>
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 6, color: card.color }}>{card.title}</div>
                <div style={{ fontSize: 11, color: "#94a3b8", lineHeight: 1.5 }}>{card.text}</div>
              </div>
            ))}
          </div>
        </div>
      </main>
    </div>
  );
}
