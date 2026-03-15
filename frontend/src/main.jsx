import React from "react";
import ReactDOM from "react-dom/client";
import "./App.css";

if (import.meta.env.DEV) {
  const originalWarn = console.warn;
  console.warn = (...args) => {
    const firstArg = args[0];
    const message = typeof firstArg === "string" ? firstArg : "";

    // Suppress known third-party deprecation noise from three/globe internals in dev.
    if (message.includes("THREE.THREE.Clock: This module has been deprecated")) {
      return;
    }

    originalWarn(...args);
  };
}

async function bootstrap() {
  const { default: App } = await import("./App.jsx");
  ReactDOM.createRoot(document.getElementById("app")).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>,
  );
}

bootstrap();
