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
- `packages/sync/protocol`: sync protocol/runtime core
- `packages/sync/material/sqlite`: SQLite-backed sync adapters and proof-material stores
- `packages/sync/material/postgres`: Postgres-backed sync proof-material stores
- `packages/sync/server/core`: shared WebSocket sync server runtime
- `packages/sync/server/postgres-node`: Node sync server wired to Postgres backend

## Quick start
```
pnpm install
pnpm build
pnpm test
```

## Playground
- Live demo (GitHub Pages): https://cybersemics.github.io/treecrdt/

### Local playground with a real sync server
If you want the most useful local setup, start the sync server first and then point the playground at it:

```sh
# Build all workspace packages, including the Postgres sync-server path.
pnpm build
# Start a disposable local Postgres instance in Docker on localhost:5432.
pnpm sync-server:postgres:db:start
# Start the TreeCRDT sync server on ws://localhost:8787 using that Postgres DB.
pnpm sync-server:postgres:local
# Start the playground UI so you can connect it to the local sync server.
pnpm playground
```

Open the `Connections` panel in the playground and set the remote sync server URL to:

```
ws://localhost:8787
```

The playground will normalize this to `/sync?docId=...`.

`pnpm sync-server:postgres:db:start` starts a disposable local Postgres on the common URL:

```
postgres://postgres:postgres@127.0.0.1:5432/postgres
```

Stop that local database later with:

```
pnpm sync-server:postgres:db:stop
```

For full sync-server configuration and environment variables, see:

- `packages/sync/server/postgres-node/README.md`
- `examples/playground/README.md`

## Contribute
Contributions are welcome!
