import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { treecrdt as treecrdtWaSqliteAssets } from "@treecrdt/wa-sqlite/vite-plugin";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  plugins: [
    treecrdtWaSqliteAssets({ outDirs: ["public/wa-sqlite", "public/base-path/wa-sqlite"] }),
    react(),
  ],
  esbuild: {
    target: "esnext",
  },
  worker: {
    format: "es",
  },
  build: {
    target: "esnext",
    rollupOptions: {
      output: {
        format: "es",
      },
    },
  },
  server: {
    port: 4166,
    fs: {
      allow: [
        __dirname,
        path.resolve(__dirname, "../../../vendor"),
        path.resolve(__dirname, "./public"),
        path.resolve(__dirname, "../dist"),
        path.resolve(__dirname, "../../treecrdt-riblt-wasm-js"),
      ],
    },
  },
  resolve: {
    alias: [
      {
        find: "wa-sqlite/sqlite-api",
        replacement: path.resolve(
          __dirname,
          "../../../vendor/wa-sqlite/src/sqlite-api.js"
        ),
      },
      {
        find: "wa-sqlite",
        replacement: path.resolve(
          __dirname,
          "../../../vendor/wa-sqlite/dist/wa-sqlite-async.mjs"
        ),
      },
    ],
  },
});
