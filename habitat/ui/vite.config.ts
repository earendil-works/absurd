import { defineConfig } from "vite";
import solid from "vite-plugin-solid";
import tailwindcss from "@tailwindcss/vite";
import path from "path";
import { consoleForwardPlugin } from "vite-console-forward-plugin";

export default defineConfig(({ mode }) => ({
  base: mode === "production" ? "/_static/" : "/",
  plugins: [
    solid(),
    tailwindcss(),
    mode === "production" ? null : consoleForwardPlugin(),
  ].filter(Boolean),
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    host: "127.0.0.1",
    port: 7891,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:7890",
        changeOrigin: true,
      },
      "/_static": {
        target: "http://127.0.0.1:7890",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "../internal/web/dist",
    emptyOutDir: true,
  },
}));
