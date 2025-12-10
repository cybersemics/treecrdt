import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  plugins: [react()],
  worker: {
    format: "es",
  },
  build: {
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
