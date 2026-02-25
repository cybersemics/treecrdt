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
- `packages/sync-server/core`: shared WebSocket sync server runtime
- `packages/sync-server/postgres-node`: Node sync server wired to Postgres backend

## Quick start
```
pnpm install
pnpm build
pnpm test
```

## Playground
- Live demo (GitHub Pages): https://cybersemics.github.io/treecrdt/

## Reference sync server (Postgres backend module)
```
pnpm sync-server:postgres:setup
TREECRDT_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres pnpm sync-server:postgres
```
## Contribute
Contributions are welcome!
