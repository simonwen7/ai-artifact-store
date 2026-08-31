import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

function readDemoMode(): "local" | "showcase" {
  const env = (globalThis as { process?: { env?: Record<string, string | undefined> } })
    .process?.env;
  const raw = env?.VITE_DEMO_MODE;
  if (typeof raw === "string" && raw.trim().toLowerCase() === "showcase") {
    return "showcase";
  }
  return "local";
}

const demoMode = readDemoMode();

const runtimeImpl =
  demoMode === "showcase"
    ? "/src/lib/showcaseRuntime.tsx"
    : "/src/lib/localRuntime.tsx";

export default defineConfig({
  plugins: [react()],
  define: {
    "import.meta.env.VITE_DEMO_MODE": JSON.stringify(demoMode),
  },
  resolve: {
    alias: {
      "@aistore/demo-runtime-impl": runtimeImpl,
    },
  },
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8787",
        changeOrigin: true,
      },
    },
  },
  preview: {
    host: "127.0.0.1",
    port: 5173,
  },
});
