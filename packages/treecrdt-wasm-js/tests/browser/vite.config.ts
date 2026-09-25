import { defineConfig } from 'vite';
import { fileURLToPath } from 'node:url';

export default defineConfig({
  root: fileURLToPath(new URL('./', import.meta.url)),
  base: '/memory/',
  optimizeDeps: { exclude: ['@treecrdt/wasm'] },
  build: { target: 'es2020', outDir: '../../.browser-test-dist', emptyOutDir: true },
});
