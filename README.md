# treecrdt

Tree CRDT workspace targeting SQLite/wa-sqlite + WASM bindings with a shared TypeScript interface.

## Layout
- `packages/treecrdt-core`: core CRDT library with traits for storage/indexing/access control.
- `packages/treecrdt-sqlite-ext`: SQLite/wa-sqlite extension harness that will implement the core traits.
- `packages/treecrdt-wasm`: bridge for wasm-bindgen and browser/node builds.
- `packages/treecrdt-wasm-js`: TS/JS wrapper for the treecrdt-wasm build
- `packages/treecrdt-ts`: TypeScript interface definitions shared by bindings and the sync layer.
- `packages/treecrdt-sqlite-node`: TreeCRDT bundled for Node.js use
- `packages/treecrdt-wa-sqlite`: TreeCRDT bunlded for browser use
- `packages/treecrdt-benchmark`: Benchmark utilities

## Quick start
```
pnpm install
pnpm build
pnpm test
```

## Playground
- Live demo (GitHub Pages): https://cybersemics.github.io/treecrdt/

## Contribute
Contributions are welcome!
