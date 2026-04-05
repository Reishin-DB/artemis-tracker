import React, { useState, useRef, useEffect } from "react";

interface Message {
  role: "user" | "assistant";
  content: string;
}

const SUGGESTIONS = [
  "What is Orion's current distance from the Moon?",
  "Which milestones are completed?",
  "What was Orion's maximum speed?",
  "Show trajectory near the Moon",
  "When is the lunar flyby?",
  "How has distance from Earth changed?",
];

const MissionAdvisor: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const sendMessage = async (text: string) => {
    if (!text.trim() || streaming) return;

    const userMsg: Message = { role: "user", content: text.trim() };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");
    setStreaming(true);

    try {
      const resp = await fetch("/api/v1/advisor", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message: text.trim(),
          conversation_id: conversationId,
        }),
      });

      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }

      const data = await resp.json();

      if (data.conversation_id) {
        setConversationId(data.conversation_id);
      }

      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: data.content || "No response." },
      ]);
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: `Error: ${err instanceof Error ? err.message : "Unknown error"}. Try again.`,
        },
      ]);
    } finally {
      setStreaming(false);
      inputRef.current?.focus();
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    sendMessage(input);
  };

  return (
    <div className="advisor">
      <div className="advisor-messages">
        {messages.length === 0 && (
          <div className="advisor-welcome">
            <div className="advisor-welcome-icon">🛰</div>
            <div className="advisor-welcome-title">Mission Advisor</div>
            <div className="advisor-welcome-powered">Powered by Genie</div>
            <div className="advisor-welcome-text">
              Ask natural language questions about Orion's trajectory, mission milestones, telemetry, and more. Genie queries the live mission database.
            </div>
            <div className="advisor-suggestions">
              {SUGGESTIONS.map((s) => (
                <button
                  key={s}
                  className="advisor-suggestion"
                  onClick={() => sendMessage(s)}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}
        {messages.map((msg, i) => (
          <div
            key={i}
            className={`advisor-msg ${msg.role}`}
          >
            <div className="advisor-msg-label">
              {msg.role === "user" ? "YOU" : "ADVISOR"}
            </div>
            <div className="advisor-msg-content">
              {msg.content || (streaming && i === messages.length - 1 ? "▊" : "")}
            </div>
          </div>
        ))}
        <div ref={messagesEndRef} />
      </div>
      <form className="advisor-input-bar" onSubmit={handleSubmit}>
        <input
          ref={inputRef}
          type="text"
          className="advisor-input"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ask the Mission Advisor..."
          disabled={streaming}
        />
        <button
          type="submit"
          className="advisor-send"
          disabled={streaming || !input.trim()}
        >
          {streaming ? "..." : "Send"}
        </button>
      </form>
    </div>
  );
};

export default MissionAdvisor;
