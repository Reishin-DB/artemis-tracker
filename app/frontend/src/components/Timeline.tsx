import React from "react";

interface Milestone {
  event: string;
  planned_time: string;
  status: "completed" | "in_progress" | "upcoming";
}

interface TimelineProps {
  milestones: Milestone[] | null;
}

function formatDate(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
  } catch {
    return iso;
  }
}

const Timeline: React.FC<TimelineProps> = ({ milestones }) => {
  if (!milestones || milestones.length === 0) {
    return (
      <div className="timeline">
        <div className="timeline-header">Mission Timeline</div>
        <div className="loading-state">No milestone data available</div>
      </div>
    );
  }

  return (
    <div className="timeline">
      <div className="timeline-header">Mission Timeline</div>
      <div className="timeline-track">
        {milestones.map((m, i) => {
          const connectorClass =
            m.status === "completed" || m.status === "in_progress"
              ? "completed"
              : "";
          return (
            <div className="timeline-item" key={i}>
              {i < milestones.length - 1 && (
                <div className={`timeline-connector ${connectorClass}`} />
              )}
              <div className={`timeline-dot ${m.status}`} />
              <div className="timeline-label">{m.event}</div>
              <div className="timeline-date">
                {formatDate(m.planned_time)}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default Timeline;
