# treecrdt

Tree CRDT workspace targeting SQLite/wa-sqlite + WASM bindings with a shared TypeScript interface.

## Layout
- `packages/treecrdt-core`: core CRDT library with traits for storage/indexing/access control.
- `packages/treecrdt-sqlite-ext`: SQLite/wa-sqlite extension harness that will implement the core traits.
- `packages/treecrdt-wasm`: bridge for wasm-bindgen and browser/node builds.
- `packages/treecrdt-ts`: TypeScript interface definitions shared by bindings and the sync layer.
- `references/`: external implementations for inspiration only (do not copy code).

## Quick start
```
pnpm install
pnpm build
pnpm test
```

## Playground
- Live demo (GitHub Pages): https://cybersemics.github.io/treecrdt/
