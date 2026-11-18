import { spawn } from "node:child_process";
import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import { staticI18nPlugin } from "./plugins/static-i18n-plugin.ts";

const DEV_PAGE = "http://127.0.0.1:3000/__byok-api__/";

const host = process.env.TAURI_DEV_HOST;

// https://vite.dev/config/
export default defineConfig(async ({ command }) => ({
  base: "/__byok-api__/",
  plugins: [
    staticI18nPlugin(),
    ...(command === "serve" ? [await codeInspector()] : []),
    ...(command === "serve" && process.env.CURSOR_OPEN_DEV_PAGE === "1" ? [openDevPagePlugin()] : []),
    react(),
  ],

  // Vite options tailored for Tauri development and only applied in `tauri dev` or `tauri build`
  //
  // 1. prevent Vite from obscuring rust errors
  clearScreen: false,
  // 2. tauri expects a fixed port, fail if that port is not available
  server: {
    port: 1420,
    strictPort: true,
    host: host || "127.0.0.1",
    hmr: host
      ? {
          protocol: "ws",
          host,
          port: 1421,
        }
      : undefined,
    watch: {
      // 3. tell Vite to ignore watching `src-tauri`
      ignored: ["**/src-tauri/**"],
    },
  },
}));

async function codeInspector(): Promise<Plugin> {
  // Node warns on the localStorage getter unless --localstorage-file is set.
  // code-inspector's bundled debug reads it at import time and only needs the browser copy.
  Object.defineProperty(globalThis, "localStorage", {
    configurable: true,
    get: () => undefined,
  });
  const { codeInspectorPlugin } = await import("code-inspector-plugin");
  return codeInspectorPlugin({
    bundler: "vite",
    editor: "cursor",
    hotKeys: ["ctrlKey"],
    pathType: "absolute",
    ...(process.platform === "darwin" ? { launchType: "open" as const } : {}),
  });
}

function openDevPagePlugin(): Plugin {
  return {
    name: "open-dev-page",
    configureServer(server) {
      server.httpServer?.once("listening", () => {
        void openWhenReady(DEV_PAGE);
      });
    },
  };
}

async function openWhenReady(url: string) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      if ((await fetch(url)).ok) {
        openUrl(url);
        return;
      }
    } catch {
      // Proxy is not accepting connections yet.
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  openUrl(url);
}

function openUrl(url: string) {
  const command = process.platform === "darwin" ? "open" : process.platform === "win32" ? "cmd" : "xdg-open";
  const args = process.platform === "win32" ? ["/c", "start", "", url] : [url];
  spawn(command, args, { stdio: "ignore", detached: true }).unref();
}
