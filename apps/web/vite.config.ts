import { execSync } from "node:child_process";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

function resolveDefaultApiProxyTarget(): string {
  if (process.env.VITE_API_PROXY_TARGET) {
    return process.env.VITE_API_PROXY_TARGET;
  }

  try {
    execSync("getent hosts api", { stdio: "ignore" });
    return "http://api:3000";
  } catch {
    return "http://127.0.0.1:3000";
  }
}

const apiProxyTarget = resolveDefaultApiProxyTarget();

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    port: 5173,
    proxy: {
      "/api": {
        target: apiProxyTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, "")
      }
    }
  }
});
