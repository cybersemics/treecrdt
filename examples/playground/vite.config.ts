import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { treecrdt } from "@treecrdt/wa-sqlite/vite-plugin";

// Setting base to "./" keeps asset paths relative, which works on GitHub Pages.
export default defineConfig({
  plugins: [treecrdt(), react()],
  base: "./",
  server: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
  },
  preview: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
  },
});
