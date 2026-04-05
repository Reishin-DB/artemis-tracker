import React, { useState, useRef, useEffect } from "react";

interface Message {
  role: "user" | "assistant";
  content: string;
}

const SUGGESTIONS = [
  "What's Orion's current status?",
  "When is the lunar flyby?",
  "Tell me about the crew",
  "What happens during re-entry?",
  "How does DSN tracking work?",
  "What's different from Apollo?",
];

const MissionAdvisor: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
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

    // Add empty assistant message for streaming
    setMessages((prev) => [...prev, { role: "assistant", content: "" }]);

    try {
      const resp = await fetch("/api/v1/advisor", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message: text.trim(),
          history: messages.slice(-10),
        }),
      });

      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }

      const reader = resp.body?.getReader();
      if (!reader) throw new Error("No response body");

      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (line.startsWith("data: ")) {
            const data = line.slice(6).trim();
            if (data === "[DONE]") break;
            try {
              const parsed = JSON.parse(data);
              if (parsed.content) {
                setMessages((prev) => {
                  const updated = [...prev];
                  const last = updated[updated.length - 1];
                  if (last && last.role === "assistant") {
                    updated[updated.length - 1] = {
                      ...last,
                      content: last.content + parsed.content,
                    };
                  }
                  return updated;
                });
              }
            } catch {
              // skip parse errors
            }
          }
        }
      }
    } catch (err) {
      setMessages((prev) => {
        const updated = [...prev];
        const last = updated[updated.length - 1];
        if (last && last.role === "assistant" && !last.content) {
          updated[updated.length - 1] = {
            ...last,
            content: `Connection error: ${err instanceof Error ? err.message : "Unknown error"}. Try again.`,
          };
        }
        return updated;
      });
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
            <div className="advisor-welcome-text">
              AI-powered mission intelligence. Ask about crew, trajectory, systems, timeline, or anything Artemis II.
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
