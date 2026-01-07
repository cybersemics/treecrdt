import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { treecrdt as treecrdtWaSqliteAssets } from "@treecrdt/wa-sqlite/vite-plugin";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const vendorPkgRoot = (() => {
  const require = createRequire(import.meta.url);
  const pkgJson = require.resolve("@treecrdt/wa-sqlite-vendor/package.json");
  return path.dirname(pkgJson);
})();
const vendorWaSqliteRoot = path.join(vendorPkgRoot, "wa-sqlite");
const vendorDistRoot = path.join(vendorPkgRoot, "dist");

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
        vendorWaSqliteRoot,
        vendorDistRoot,
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
          vendorWaSqliteRoot,
          "src",
          "sqlite-api.js"
        ),
      },
      {
        find: "wa-sqlite",
        replacement: path.resolve(
          __dirname,
          vendorDistRoot,
          "wa-sqlite-async.mjs"
        ),
      },
    ],
  },
});
