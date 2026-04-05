import React from "react";

interface CrewPanelProps {
  phase: string;
}

interface CrewMember {
  name: string;
  role: string;
  agency: string;
}

const CREW: CrewMember[] = [
  { name: "Reid Wiseman", role: "Commander", agency: "NASA" },
  { name: "Victor Glover", role: "Pilot", agency: "NASA" },
  { name: "Christina Koch", role: "Mission Specialist 1", agency: "NASA" },
  { name: "Jeremy Hansen", role: "Mission Specialist 2", agency: "CSA" },
];

/** Derive a plausible crew activity description from mission phase. */
function getCrewActivity(phase: string): string {
  const p = phase.toLowerCase();
  if (p.includes("launch") || p.includes("ascent"))
    return "Launch and ascent — crew monitoring vehicle performance";
  if (p.includes("tli") || p.includes("trans-lunar"))
    return "Post-TLI checkout, stowing launch equipment";
  if (p.includes("coast") || p.includes("transit"))
    return "Deep-space transit — spacecraft systems checks, crew rest rotation";
  if (p.includes("flyby") || p.includes("lunar"))
    return "Lunar flyby — crew observing and photographing the far side";
  if (p.includes("return") || p.includes("tei"))
    return "Return transit — preparing for Earth re-entry procedures";
  if (p.includes("entry") || p.includes("reentry") || p.includes("ei"))
    return "Entry interface — crew secured for atmospheric re-entry";
  if (p.includes("splash") || p.includes("recovery"))
    return "Post-splashdown — awaiting recovery team";
  return "Nominal operations — crew performing scheduled activities";
}

const CrewPanel: React.FC<CrewPanelProps> = ({ phase }) => {
  const activity = getCrewActivity(phase);

  return (
    <div className="cc-crew">
      <div className="cc-section-header">Crew Status</div>
      <div className="cc-crew-list">
        {CREW.map((member) => (
          <div className="cc-crew-member" key={member.name}>
            <div className="cc-crew-avatar">
              <span className="cc-crew-initials">
                {member.name
                  .split(" ")
                  .map((n) => n[0])
                  .join("")}
              </span>
            </div>
            <div className="cc-crew-info">
              <span className="cc-crew-name">{member.name}</span>
              <span className="cc-crew-role">
                {member.role} &middot; {member.agency}
              </span>
            </div>
            <span className="cc-crew-status-dot" title="Nominal" />
          </div>
        ))}
      </div>
      <div className="cc-crew-activity">
        <span className="cc-crew-activity-label">Current Activity</span>
        <span className="cc-crew-activity-text">{activity}</span>
      </div>
    </div>
  );
};

export default CrewPanel;
