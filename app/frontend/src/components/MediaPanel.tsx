import React from "react";

interface MediaItem {
  title: string;
  url: string;
  thumbnail_url?: string;
  media_type?: string;
  date_created?: string;
}

interface MediaPanelProps {
  mediaData: MediaItem[] | null;
  nasaTvEmbed?: string;
}

const MediaPanel: React.FC<MediaPanelProps> = ({ mediaData, nasaTvEmbed }) => {
  const embedUrl = nasaTvEmbed || "https://www.youtube.com/embed/live_stream?channel=UCLA_DiR1FfKNvjuUpBHmylQ";
  const items = mediaData?.slice(0, 4) || [];

  return (
    <div className="bottom-grid">
      <div className="media-grid">
        <div className="media-grid-header">Latest Images</div>
        <div className="media-thumbnails">
          {items.length > 0
            ? items.map((item, i) => (
                <div key={i}>
                  <a
                    href={item.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="media-thumb"
                    style={{ display: "block" }}
                  >
                    {item.thumbnail_url ? (
                      <img
                        src={item.thumbnail_url}
                        alt={item.title}
                        loading="lazy"
                      />
                    ) : (
                      <div className="media-thumb-placeholder">
                        No preview
                      </div>
                    )}
                  </a>
                  <div className="media-caption">{item.title}</div>
                </div>
              ))
            : Array.from({ length: 4 }).map((_, i) => (
                <div key={i}>
                  <div className="media-thumb">
                    <div className="media-thumb-placeholder">
                      Awaiting imagery
                    </div>
                  </div>
                </div>
              ))}
        </div>
      </div>

      <div className="live-banner">
        <div className="live-banner-header">NASA TV Live</div>
        <iframe
          src={`${embedUrl}${embedUrl.includes('?') ? '&' : '?'}autoplay=0`}
          title="NASA TV Live"
          allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
          allowFullScreen
        />
        <a
          href="https://www.nasa.gov/live"
          target="_blank"
          rel="noopener noreferrer"
          className="nasa-tv-link"
        >
          <svg
            width="14"
            height="14"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
            <polyline points="15 3 21 3 21 9" />
            <line x1="10" y1="14" x2="21" y2="3" />
          </svg>
          Watch on nasa.gov/live
        </a>
      </div>
    </div>
  );
};

export default MediaPanel;
