import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const vendorRoot = (() => {
  const require = createRequire(import.meta.url);
  const pkgJson = require.resolve("@treecrdt/wa-sqlite-vendor/package.json");
  return path.join(path.dirname(pkgJson), "wa-sqlite");
})();

export default defineConfig({
  plugins: [react()],
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
        vendorRoot,
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
          vendorRoot,
          "src/sqlite-api.js"
        ),
      },
      {
        find: "wa-sqlite",
        replacement: path.resolve(
          __dirname,
          vendorRoot,
          "dist/wa-sqlite-async.mjs"
        ),
      },
    ],
  },
});
