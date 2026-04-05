import React, { useMemo } from "react";

interface CrewPanelProps {
  phase: string;
}

interface CrewMember {
  name: string;
  role: string;
  agency: string;
  initials: string;
}

const CREW: CrewMember[] = [
  { name: "Reid Wiseman", role: "Commander", agency: "NASA", initials: "RW" },
  { name: "Victor Glover", role: "Pilot", agency: "NASA", initials: "VG" },
  { name: "Christina Koch", role: "Mission Specialist 1", agency: "NASA", initials: "CK" },
  { name: "Jeremy Hansen", role: "Mission Specialist 2", agency: "CSA", initials: "JH" },
];

/* Real Artemis II flight day activities from NASA daily agenda */
interface FlightDayInfo {
  day: number;
  date: string;
  title: string;
  activities: { time: string; text: string; crew?: string }[];
}

const FLIGHT_DAYS: FlightDayInfo[] = [
  {
    day: 1, date: "Apr 1", title: "Launch & Checkout",
    activities: [
      { time: "22:35", text: "SLS liftoff from KSC LC-39B", crew: "All" },
      { time: "22:43", text: "Main engine cutoff, ICPS separation", crew: "All" },
      { time: "23:24", text: "Perigee-raising burns", crew: "CK" },
      { time: "POST", text: "Orion systems checkout: water, toilet, CO₂ removal", crew: "RW, VG" },
      { time: "POST", text: "Proximity ops demo with ICPS as docking target", crew: "VG" },
      { time: "POST", text: "Emergency comm check on Deep Space Network", crew: "CK" },
    ],
  },
  {
    day: 2, date: "Apr 2", title: "TLI & Outbound",
    activities: [
      { time: "AM", text: "Flywheel exercise device setup & first workouts", crew: "RW, VG" },
      { time: "PM", text: "Afternoon exercise session", crew: "CK, JH" },
      { time: "PM", text: "Trans-Lunar Injection burn preparation & execution", crew: "CK" },
      { time: "EVE", text: "Post-TLI acclimation, first space-to-ground video call", crew: "All" },
    ],
  },
  {
    day: 3, date: "Apr 3", title: "Deep Space Transit",
    activities: [
      { time: "AM", text: "First outbound trajectory correction burn", crew: "JH" },
      { time: "AM", text: "CPR procedure demonstrations", crew: "VG, CK, JH" },
      { time: "MID", text: "Medical kit checkout: BP monitor, stethoscope, otoscope", crew: "CK" },
      { time: "PM", text: "DSN emergency communications system test", crew: "CK" },
      { time: "PM", text: "Rehearsal for lunar observation choreography", crew: "All" },
    ],
  },
  {
    day: 4, date: "Apr 4", title: "Manual Piloting Demo",
    activities: [
      { time: "AM", text: "24-hour acoustics test of spacecraft sound environment", crew: "All" },
      { time: "AM", text: "Review lunar surface geography targets for imaging", crew: "All" },
      { time: "MID", text: "Saliva samples for AVATAR immune biomarker research", crew: "All" },
      { time: "21:10", text: "Manual piloting demonstration — Glover takes controls", crew: "VG" },
      { time: "PM", text: "Celestial body photography session (20 min)", crew: "All" },
      { time: "PM", text: "Optical comm system transmitted 100+ GB data", crew: "" },
    ],
  },
  {
    day: 5, date: "Apr 5", title: "Lunar Sphere of Influence",
    activities: [
      { time: "12:00", text: "Crew wake — Orion entering lunar sphere of influence", crew: "All" },
      { time: "AM", text: "Spacesuit testing: don, pressurize, seat install", crew: "All" },
      { time: "AM", text: "Suit eat/drink port testing", crew: "RW, VG" },
      { time: "PM", text: "Final outbound trajectory correction burn", crew: "JH" },
      { time: "PM", text: "Pre-flyby systems review and camera prep", crew: "CK" },
    ],
  },
  {
    day: 6, date: "Apr 6", title: "LUNAR FLYBY",
    activities: [
      { time: "14:45", text: "Lunar flyby window opens — closest approach ~6,400 km", crew: "All" },
      { time: "FLY", text: "Photography: craters, lava flows, surface cracks, ridges", crew: "All" },
      { time: "FLY", text: "30-50 min comm blackout behind the Moon", crew: "All" },
      { time: "21:40", text: "Flyby window closes — return trajectory established", crew: "All" },
    ],
  },
  {
    day: 7, date: "Apr 7", title: "Off-Duty / Debrief",
    activities: [
      { time: "AM", text: "Exit lunar sphere of influence", crew: "" },
      { time: "AM", text: "Crew debriefing with ground scientists", crew: "All" },
      { time: "MID", text: "First return trajectory correction burn", crew: "CK" },
      { time: "PM", text: "Off-duty rest period", crew: "All" },
    ],
  },
  {
    day: 8, date: "Apr 8", title: "Radiation & Piloting Tests",
    activities: [
      { time: "AM", text: "Solar flare radiation shelter construction demo", crew: "All" },
      { time: "PM", text: "Manual piloting: 6-DOF vs 3-DOF attitude control", crew: "VG, RW" },
    ],
  },
  {
    day: 9, date: "Apr 9", title: "Re-entry Prep",
    activities: [
      { time: "AM", text: "Re-entry and splashdown procedure study", crew: "All" },
      { time: "MID", text: "Second return trajectory correction burn", crew: "JH" },
      { time: "PM", text: "Waste collection systems demo", crew: "RW" },
      { time: "PM", text: "Orthostatic intolerance garment fit checks", crew: "All" },
    ],
  },
  {
    day: 10, date: "Apr 10", title: "SPLASHDOWN",
    activities: [
      { time: "AM", text: "Final return trajectory correction burn", crew: "CK" },
      { time: "AM", text: "Cabin reconfiguration and equipment stowage", crew: "All" },
      { time: "MID", text: "Crew returns to spacesuits, service module sep", crew: "All" },
      { time: "16:00", text: "Entry interface — 25,000 mph, heat shield 2,760°C", crew: "All" },
      { time: "16:45", text: "Parachute deploy: 2 drogue → 3 pilot → 3 main", crew: "" },
      { time: "17:00", text: "SPLASHDOWN — Pacific Ocean recovery", crew: "All" },
    ],
  },
];

const CrewPanel: React.FC<CrewPanelProps> = ({ phase: _phase }) => {
  const now = useMemo(() => new Date(), []);
  const launchTime = new Date("2026-04-01T22:35:00Z");
  const elapsedMs = now.getTime() - launchTime.getTime();
  const currentDay = Math.max(1, Math.min(10, Math.ceil(elapsedMs / 86400000)));

  const todayInfo = FLIGHT_DAYS.find((d) => d.day === currentDay) || FLIGHT_DAYS[0];
  const nextDayInfo = FLIGHT_DAYS.find((d) => d.day === currentDay + 1);

  return (
    <div className="cc-crew">
      <div className="cc-section-header">Crew Status — Flight Day {currentDay}</div>

      {/* Crew roster */}
      <div className="cc-crew-list">
        {CREW.map((member) => (
          <div className="cc-crew-member" key={member.name}>
            <div className="cc-crew-avatar">
              <span className="cc-crew-initials">{member.initials}</span>
            </div>
            <div className="cc-crew-info">
              <span className="cc-crew-name">{member.name}</span>
              <span className="cc-crew-role">{member.role} · {member.agency}</span>
            </div>
            <span className="cc-crew-status-dot" title="Nominal" />
          </div>
        ))}
      </div>

      {/* Today's activities */}
      <div className="cc-crew-schedule">
        <div className="cc-crew-day-header">
          <span className="cc-crew-day-badge">DAY {todayInfo.day}</span>
          <span className="cc-crew-day-title">{todayInfo.title}</span>
        </div>
        <div className="cc-crew-activities">
          {todayInfo.activities.map((a, i) => (
            <div className="cc-crew-activity-row" key={i}>
              <span className="cc-crew-time">{a.time}</span>
              <span className="cc-crew-activity-text">{a.text}</span>
              {a.crew && <span className="cc-crew-who">{a.crew}</span>}
            </div>
          ))}
        </div>
      </div>

      {/* Next day preview */}
      {nextDayInfo && (
        <div className="cc-crew-next">
          <span className="cc-crew-next-label">NEXT: Day {nextDayInfo.day} — {nextDayInfo.title}</span>
          <span className="cc-crew-next-preview">{nextDayInfo.activities[0]?.text}</span>
        </div>
      )}
    </div>
  );
};

export default CrewPanel;
