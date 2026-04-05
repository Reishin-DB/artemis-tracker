import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import CommandCenterPage from "./pages/CommandCenterPage";
import AdminPage from "./pages/AdminPage";
import ErrorBoundary from "./components/ErrorBoundary";
import "./index.css";

// Capture all global errors
window.addEventListener('error', (e) => {
  console.error('[GLOBAL ERROR]', e.message, e.filename + ':' + e.lineno);
});

window.addEventListener('unhandledrejection', (e) => {
  console.error('[UNHANDLED REJECTION]', e.reason);
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ErrorBoundary>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<CommandCenterPage />} />
          <Route path="/command-center" element={<CommandCenterPage />} />
          <Route path="/admin" element={<AdminPage />} />
        </Routes>
      </BrowserRouter>
    </ErrorBoundary>
  </React.StrictMode>
);
