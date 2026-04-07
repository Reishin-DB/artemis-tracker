import React, { useState } from "react";
import { Link, useLocation } from "react-router-dom";
import usePolling from "../hooks/usePolling";

/* ── Types ──────────────────────────────────────────────────── */

interface SourceHealth {
  source_name: string;
  health: string;
  last_ingest_utc: string | null;
  seconds_since_last_ingest: number | null;
  records_last_hour: number | null;
  parse_errors_last_hour: number | null;
  avg_latency_ms: number | null;
  detail: string | null;
}

interface Alert {
  severity: string;
  source: string;
  message: string;
  since: string | null;
}

interface DiagnosticsData {
  sources: SourceHealth[];
  alerts: Alert[];
}

/* ── Helpers ─────────────────────────────────────────────────── */

function healthBadgeClass(health: string): string {
  if (health === "healthy") return "green";
  if (health === "warning") return "amber";
  return "red";
}

function formatTimestamp(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("en-US", {
      month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
    });
  } catch { return String(iso); }
}

function num(v: number | null | undefined, fallback = 0): number {
  return v != null ? v : fallback;
}

/* ── Data Flow SVG ───────────────────────────────────────────── */

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
  <rect width="960" height="640" fill="#060a12" rx="12"/>

  <text x="20" y="28" class="label">REAL-TIME NASA DATA SOURCES</text>
  <rect x="30" y="40" width="200" height="56" class="box" fill="#1e293b" stroke="#10b981" stroke-width="1.5"/>
  <text x="130" y="62" text-anchor="middle" class="title" style="fill:#10b981">JPL Horizons API</text>
  <text x="130" y="78" text-anchor="middle" class="subtitle">Orion vectors (COMMAND=-1024)</text>
  <text x="130" y="90" text-anchor="middle" class="subtitle" style="fill:#10b981;font-size:8px">Every 5 min | JSON/CSV</text>

  <rect x="260" y="40" width="200" height="56" class="box" fill="#1e293b" stroke="#3b82f6" stroke-width="1.5"/>
  <text x="360" y="62" text-anchor="middle" class="title" style="fill:#3b82f6">AROW Ephemeris</text>
  <text x="360" y="78" text-anchor="middle" class="subtitle">CCSDS OEM state vectors</text>
  <text x="360" y="90" text-anchor="middle" class="subtitle" style="fill:#3b82f6;font-size:8px">Periodic | J2000 4-min intervals</text>

  <rect x="490" y="40" width="200" height="56" class="box" fill="#1e293b" stroke="#f59e0b" stroke-width="1.5"/>
  <text x="590" y="62" text-anchor="middle" class="title" style="fill:#f59e0b">NASA Image API</text>
  <text x="590" y="78" text-anchor="middle" class="subtitle">Artemis II media catalog</text>
  <text x="590" y="90" text-anchor="middle" class="subtitle" style="fill:#f59e0b;font-size:8px">Every 15 min | REST JSON</text>

  <rect x="720" y="40" width="210" height="56" class="box" fill="#1e293b" stroke="#8b5cf6" stroke-width="1.5"/>
  <text x="825" y="62" text-anchor="middle" class="title" style="fill:#8b5cf6">DSN Now + NASA Blog</text>
  <text x="825" y="78" text-anchor="middle" class="subtitle">Antenna status + mission updates</text>
  <text x="825" y="90" text-anchor="middle" class="subtitle" style="fill:#8b5cf6;font-size:8px">30 sec / 10 min | XML / RSS</text>

  <path d="M130,96 L130,155" class="edge-track"/><path d="M130,96 L130,155" class="edge edge-animated" stroke="#10b981"/>
  <path d="M360,96 L310,155" class="edge-track"/><path d="M360,96 L310,155" class="edge edge-animated" stroke="#3b82f6"/>
  <path d="M590,96 L690,155" class="edge-track"/><path d="M590,96 L690,155" class="edge edge-animated" stroke="#f59e0b"/>
  <path d="M825,96 L860,155" class="edge-track"/><path d="M825,96 L860,155" class="edge edge-animated" stroke="#8b5cf6"/>

  <text x="20" y="145" class="label">DATABRICKS LAKEHOUSE (UNITY CATALOG)</text>

  <rect x="30" y="158" width="210" height="80" class="box" fill="#0f172a" stroke="#10b981" stroke-width="1.5"/>
  <text x="135" y="178" text-anchor="middle" class="title" style="fill:#10b981">BRONZE</text>
  <text x="135" y="192" text-anchor="middle" class="subtitle">Raw append-only ingestion</text>
  <text x="48" y="208" class="subtitle" style="font-size:8px;fill:#64748b">raw_horizons_vectors</text>
  <text x="48" y="220" class="subtitle" style="font-size:8px;fill:#64748b">raw_arow_ephemeris</text>
  <text x="48" y="232" class="subtitle" style="font-size:8px;fill:#64748b">raw_nasa_media</text>

  <rect x="270" y="158" width="210" height="80" class="box" fill="#0f172a" stroke="#3b82f6" stroke-width="1.5"/>
  <text x="375" y="178" text-anchor="middle" class="title" style="fill:#3b82f6">SILVER</text>
  <text x="375" y="192" text-anchor="middle" class="subtitle">Normalized, typed, deduped</text>
  <text x="288" y="208" class="subtitle" style="font-size:8px;fill:#64748b">telemetry_normalized</text>
  <text x="288" y="220" class="subtitle" style="font-size:8px;fill:#64748b">mission_events</text>
  <text x="288" y="232" class="subtitle" style="font-size:8px;fill:#64748b">data_quality_log</text>

  <rect x="510" y="158" width="210" height="80" class="box" fill="#0f172a" stroke="#f59e0b" stroke-width="1.5"/>
  <text x="615" y="178" text-anchor="middle" class="title" style="fill:#f59e0b">GOLD</text>
  <text x="615" y="192" text-anchor="middle" class="subtitle">Business-ready views</text>
  <text x="528" y="208" class="subtitle" style="font-size:8px;fill:#64748b">current_status</text>
  <text x="528" y="220" class="subtitle" style="font-size:8px;fill:#64748b">trajectory_history</text>
  <text x="528" y="232" class="subtitle" style="font-size:8px;fill:#64748b">milestones / diagnostics</text>

  <rect x="750" y="158" width="180" height="80" class="box" fill="#0f172a" stroke="#FC3D21" stroke-width="1.5"/>
  <text x="840" y="178" text-anchor="middle" class="title" style="fill:#FC3D21">LAKEBASE</text>
  <text x="840" y="192" text-anchor="middle" class="subtitle">Synced Tables (CONTINUOUS)</text>
  <text x="768" y="208" class="subtitle" style="font-size:8px;fill:#64748b">Auto Delta → Postgres sync</text>
  <text x="768" y="220" class="subtitle" style="font-size:8px;fill:#64748b">current_status, trajectory</text>
  <text x="768" y="232" class="subtitle" style="font-size:8px;fill:#64748b">milestones | CDF enabled</text>

  <path d="M240,198 L270,198" class="edge-track"/><path d="M240,198 L270,198" class="edge edge-animated" stroke="#3b82f6"/>
  <path d="M480,198 L510,198" class="edge-track"/><path d="M480,198 L510,198" class="edge edge-animated" stroke="#f59e0b"/>
  <path d="M720,198 L750,198" class="edge-track"/><path d="M720,198 L750,198" class="edge edge-animated" stroke="#FC3D21"/>

  <rect x="20" y="255" width="920" height="55" rx="8" fill="none" stroke="#FC3D21" stroke-width="2" stroke-dasharray="6 3" opacity="0.6"/>
  <rect x="300" y="248" width="360" height="18" rx="5" fill="#060a12"/>
  <text x="480" y="262" text-anchor="middle" style="font-size:11px;font-weight:700;fill:#FC3D21;">UNITY CATALOG GOVERNANCE + DATABRICKS WORKFLOWS</text>
  <text x="80" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">Service Principals</text>
  <text x="240" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">GRANT / DENY</text>
  <text x="400" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">Data Quality Monitors</text>
  <text x="570" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">Scheduled Jobs (5 min)</text>
  <text x="740" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">SQL Alerts</text>
  <text x="880" y="285" text-anchor="middle" class="subtitle" style="font-size:9px;">Lineage</text>

  <text x="20" y="335" class="label">FASTAPI BACKEND (DATABRICKS APP) — 7 ENDPOINTS</text>
  <rect x="30" y="348" width="130" height="48" class="box" fill="#1e293b" stroke="#10b981" stroke-width="1"/>
  <text x="95" y="367" text-anchor="middle" class="title">/current</text>
  <text x="95" y="381" text-anchor="middle" class="subtitle">30s | live position</text>
  <text x="95" y="392" text-anchor="middle" class="subtitle" style="fill:#10b981;font-size:7px">DB → Horizons fallback</text>

  <rect x="170" y="348" width="130" height="48" class="box" fill="#1e293b" stroke="#3b82f6" stroke-width="1"/>
  <text x="235" y="367" text-anchor="middle" class="title">/path</text>
  <text x="235" y="381" text-anchor="middle" class="subtitle">5m | CatmullRom curve</text>
  <text x="235" y="392" text-anchor="middle" class="subtitle" style="fill:#3b82f6;font-size:7px">833 pts | full loop</text>

  <rect x="310" y="348" width="130" height="48" class="box" fill="#1e293b" stroke="#f59e0b" stroke-width="1"/>
  <text x="375" y="367" text-anchor="middle" class="title">/milestones</text>
  <text x="375" y="381" text-anchor="middle" class="subtitle">5m | 9 events</text>
  <text x="375" y="392" text-anchor="middle" class="subtitle" style="fill:#f59e0b;font-size:7px">DB → hardcoded fallback</text>

  <rect x="450" y="348" width="130" height="48" class="box" fill="#1e293b" stroke="#8b5cf6" stroke-width="1"/>
  <text x="515" y="367" text-anchor="middle" class="title">/diagnostics</text>
  <text x="515" y="381" text-anchor="middle" class="subtitle">30s | 6 sources</text>
  <text x="515" y="392" text-anchor="middle" class="subtitle" style="fill:#8b5cf6;font-size:7px">Computed from endpoints</text>

  <rect x="590" y="348" width="130" height="48" class="box" fill="#1e293b" stroke="#FC3D21" stroke-width="1"/>
  <text x="655" y="367" text-anchor="middle" class="title">/advisor</text>
  <text x="655" y="381" text-anchor="middle" class="subtitle">Powered by Genie</text>
  <text x="655" y="392" text-anchor="middle" class="subtitle" style="fill:#FC3D21;font-size:7px">NL → SQL → English answer</text>

  <rect x="730" y="348" width="120" height="48" class="box" fill="#1e293b" stroke="#64748b" stroke-width="1"/>
  <text x="790" y="367" text-anchor="middle" class="title">/media</text>
  <text x="790" y="381" text-anchor="middle" class="subtitle">10m | NASA API</text>

  <rect x="860" y="348" width="80" height="48" class="box" fill="#1e293b" stroke="#64748b" stroke-width="1"/>
  <text x="900" y="367" text-anchor="middle" class="title">/health</text>
  <text x="900" y="381" text-anchor="middle" class="subtitle">Backend info</text>

  <path d="M840,238 L790,348" class="edge-track"/><path d="M840,238 L790,348" class="edge edge-animated" stroke="#FC3D21"/>
  <path d="M615,238 L375,348" class="edge-track"/><path d="M615,238 L375,348" class="edge edge-animated" stroke="#f59e0b"/>
  <path d="M615,238 L235,348" class="edge-track"/><path d="M615,238 L235,348" class="edge edge-animated" stroke="#3b82f6"/>
  <path d="M135,238 L95,348" class="edge-track"/><path d="M135,238 L95,348" class="edge edge-animated" stroke="#10b981"/>

  <text x="20" y="425" class="label">REACT FRONTEND (2 VIEWS)</text>
  <rect x="30" y="438" width="440" height="70" class="box" fill="#1a0f0a" stroke="#f59e0b" stroke-width="1.5"/>
  <text x="250" y="458" text-anchor="middle" class="title" style="fill:#f59e0b">MISSION CONTROL CENTER</text>
  <text x="250" y="473" text-anchor="middle" class="subtitle">3D CatmullRom orbit | Interactive digital twin | NASA Live PiP</text>
  <text x="250" y="486" text-anchor="middle" class="subtitle">Mission Advisor (Genie) | DSN comms | Crew schedule | Timeline</text>
  <text x="250" y="499" text-anchor="middle" class="subtitle" style="fill:#f59e0b;font-size:7px">Systems bar | Telemetry ticker | Error boundaries</text>

  <rect x="490" y="438" width="440" height="70" class="box" fill="#10091a" stroke="#8b5cf6" stroke-width="1.5"/>
  <text x="710" y="458" text-anchor="middle" class="title" style="fill:#8b5cf6">OPERATIONS CENTER</text>
  <text x="710" y="473" text-anchor="middle" class="subtitle">Pipeline diagnostics (6 live sources) | Data + AI flow diagram</text>
  <text x="710" y="486" text-anchor="middle" class="subtitle">Source health cards | Active alerts | Architecture overview</text>
  <text x="710" y="499" text-anchor="middle" class="subtitle" style="fill:#8b5cf6;font-size:7px">Tabbed navigation | Responsive layout</text>

  <path d="M235,396 L250,438" class="edge-track"/><path d="M235,396 L250,438" class="edge edge-animated" stroke="#f59e0b"/>
  <path d="M515,396 L710,438" class="edge-track"/><path d="M515,396 L710,438" class="edge edge-animated" stroke="#8b5cf6"/>

  <text x="20" y="540" class="label">RESILIENCE + AUTOMATION</text>
  <rect x="30" y="553" width="450" height="40" class="box" fill="#0f172a" stroke="#10b981" stroke-width="1" stroke-dasharray="4 2"/>
  <text x="255" y="570" text-anchor="middle" class="title" style="fill:#10b981">Horizons Live Fallback — every endpoint has DB → API → hardcoded safety net</text>
  <text x="255" y="583" text-anchor="middle" class="subtitle">No mocks. No simulated data. Real NASA telemetry always.</text>

  <rect x="500" y="553" width="430" height="40" class="box" fill="#0f172a" stroke="#f59e0b" stroke-width="1" stroke-dasharray="4 2"/>
  <text x="715" y="570" text-anchor="middle" class="title" style="fill:#f59e0b">Genie Auto-Ingestion — scheduled notebook writes to UC + Lakebase</text>
  <text x="715" y="583" text-anchor="middle" class="subtitle">Every 5 min | Serverless compute</text>

  <text x="480" y="620" text-anchor="middle" style="font-size:10px;fill:#475569;">
    Artemis II Mission Tracker — Databricks | Lakebase | Unity Catalog | JPL Horizons | Foundation Models | React Three Fiber
  </text>
</svg>`;

const HOW_IT_WORKS = [
  { icon: "🛰", title: "Real NASA Data Sources", text: "JPL Horizons API provides Orion + Moon state vectors. NASA Image API supplies media. DSN status computed from station geometry. All real — no mocks.", color: "#10b981" },
  { icon: "📐", title: "Medallion Lakehouse", text: "Bronze (raw API responses) → Silver (normalized, deduped) → Gold (business views) in Unity Catalog. Genie-generated ingestion notebook keeps data fresh every 5 min.", color: "#3b82f6" },
  { icon: "🐘", title: "Lakebase Synced Tables", text: "CONTINUOUS sync from UC Delta tables to Lakebase Postgres via Change Data Feed. 3 synced tables auto-replicate. No manual ETL — Databricks handles it.", color: "#FC3D21" },
  { icon: "🤖", title: "Mission Advisor (Genie)", text: "Powered by Databricks Genie. Natural language questions → SQL queries → English answers from live UC mission tables. Maintains conversation context for follow-ups.", color: "#f59e0b" },
  { icon: "🔄", title: "Triple Fallback", text: "Every endpoint: try DB first → Horizons live API → hardcoded real data. Error boundaries catch render failures. Diagnostics computed from cached endpoint responses.", color: "#8b5cf6" },
  { icon: "🌍", title: "3D Orbital Visualization", text: "CatmullRom spline interpolation renders smooth elliptical trajectory. 833 points from launch to splashdown. Interactive digital twin with clickable specs. NASA Live PiP.", color: "#10b981" },
];

/* ── Component ──────────────────────────────────────────────── */

const AdminPage: React.FC = () => {
  const location = useLocation();
  const [tab, setTab] = useState<"diagnostics" | "dataflow">("diagnostics");
  const { data, loading, error } = usePolling<DiagnosticsData>("/api/v1/diagnostics", 30_000);

  const sources = data?.sources ?? [];
  const alerts = data?.alerts ?? [];
  const totalRecords = sources.reduce((s, r) => s + num(r.records_last_hour), 0);
  const totalErrors = sources.reduce((s, r) => s + num(r.parse_errors_last_hour), 0);
  const healthyCount = sources.filter((s) => s.health === "healthy").length;

  return (
    <div className="app mcc-app">
      <header className="mcc-header">
        <div className="mcc-header-left">
          <div className="mcc-header-text">
            <span className="mcc-title">ARTEMIS II</span>
            <span className="mcc-subtitle">DIAGNOSTICS</span>
          </div>
        </div>
        <div className="mcc-header-right">
          <Link to="/" className={`mcc-nav-link${location.pathname === "/" ? " active" : ""}`}>
            Mission Control
          </Link>
          <Link to="/admin" className={`mcc-nav-link${location.pathname === "/admin" ? " active" : ""}`}>
            Operations
          </Link>
        </div>
      </header>

      <main className="main-content" style={{ paddingTop: 68 }}>
        {/* Tab bar */}
        <div className="admin-tabs">
          <button
            className={`admin-tab${tab === "diagnostics" ? " active" : ""}`}
            onClick={() => setTab("diagnostics")}
          >
            Pipeline Diagnostics
          </button>
          <button
            className={`admin-tab${tab === "dataflow" ? " active" : ""}`}
            onClick={() => setTab("dataflow")}
          >
            Data &amp; AI Flow
          </button>
        </div>

        {/* ── Tab: Diagnostics ─────────────────────────────── */}
        {tab === "diagnostics" && (
          <>
            {loading && !data && (
              <div className="loading-state">
                <span className="loading-spinner" />
                Loading diagnostics...
              </div>
            )}

            {error && !data && (
              <div className="error-state">Diagnostics unavailable: {error}</div>
            )}

            {data && (
              <>
                <div className="metrics-grid">
                  <div className="metric-card">
                    <div className="metric-value">{sources.length}</div>
                    <div className="metric-label">Data Sources</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-value" style={{ color: healthyCount === sources.length ? "var(--green)" : "var(--amber)" }}>
                      {healthyCount}/{sources.length}
                    </div>
                    <div className="metric-label">Healthy</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-value">{totalRecords.toLocaleString()}</div>
                    <div className="metric-label">Total Records</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-value" style={{ color: totalErrors > 0 ? "var(--red)" : "var(--text-primary)" }}>
                      {totalErrors}
                    </div>
                    <div className="metric-label">Errors</div>
                  </div>
                </div>

                <div className="admin-grid">
                  {sources.map((source) => (
                    <div className="diag-card" key={source.source_name}>
                      <div className="diag-card-header">
                        <span className="diag-card-title">{source.source_name}</span>
                        <span className={`health-badge ${healthBadgeClass(source.health)}`}>
                          <span className="health-badge-dot" />
                          {source.health}
                        </span>
                      </div>
                      {source.last_ingest_utc && (
                        <div className="diag-row">
                          <span className="diag-row-label">Last Update</span>
                          <span className="diag-row-value">{formatTimestamp(source.last_ingest_utc)}</span>
                        </div>
                      )}
                      {source.seconds_since_last_ingest != null && (
                        <div className="diag-row">
                          <span className="diag-row-label">Age</span>
                          <span className="diag-row-value">
                            {source.seconds_since_last_ingest < 60
                              ? `${source.seconds_since_last_ingest}s`
                              : source.seconds_since_last_ingest < 3600
                                ? `${Math.round(source.seconds_since_last_ingest / 60)}m`
                                : `${Math.round(source.seconds_since_last_ingest / 3600)}h`}
                          </span>
                        </div>
                      )}
                      <div className="diag-row">
                        <span className="diag-row-label">Records</span>
                        <span className="diag-row-value">{num(source.records_last_hour).toLocaleString()}</span>
                      </div>
                      {source.detail && (
                        <div className="diag-row">
                          <span className="diag-row-label">Detail</span>
                          <span className="diag-row-value" style={{ fontSize: "0.7rem", color: "var(--text-muted)" }}>{source.detail}</span>
                        </div>
                      )}
                    </div>
                  ))}
                </div>

                <div className="alerts-panel">
                  <div className="alerts-header">Active Alerts</div>
                  {alerts.length === 0 ? (
                    <div className="no-alerts">No active alerts. All systems nominal.</div>
                  ) : (
                    alerts.map((alert, i) => (
                      <div className="alert-item" key={i}>
                        <span className={`alert-severity ${alert.severity}`}>{alert.severity}</span>
                        <span className="alert-message">{alert.message}</span>
                        <span className="alert-time">{formatTimestamp(alert.since)}</span>
                      </div>
                    ))
                  )}
                </div>
              </>
            )}
          </>
        )}

        {/* ── Tab: Data & AI Flow ──────────────────────────── */}
        {tab === "dataflow" && (
          <>
            <div
              dangerouslySetInnerHTML={{ __html: SVG_CONTENT }}
              style={{ borderRadius: 12, overflow: "hidden", border: "1px solid #1a2540", maxWidth: 960, margin: "0 auto" }}
            />
            <div style={{ maxWidth: 960, margin: "20px auto 0" }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: "#94a3b8", letterSpacing: 1, textTransform: "uppercase", marginBottom: 12 }}>
                HOW IT WORKS
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
                {HOW_IT_WORKS.map((card) => (
                  <div key={card.title} style={{
                    background: "#0c1322", border: "1px solid #1a2540", borderRadius: 8, padding: 14,
                    borderLeft: `3px solid ${card.color}`,
                  }}>
                    <div style={{ fontSize: 20, marginBottom: 6 }}>{card.icon}</div>
                    <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 6, color: card.color }}>{card.title}</div>
                    <div style={{ fontSize: 11, color: "#94a3b8", lineHeight: 1.5 }}>{card.text}</div>
                  </div>
                ))}
              </div>
            </div>
          </>
        )}

      </main>
    </div>
  );
};

export default AdminPage;
