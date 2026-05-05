# treecrdt

Tree CRDT workspace targeting SQLite/wa-sqlite + WASM bindings with a shared TypeScript interface.

## Layout

- `packages/treecrdt-core`: core CRDT library with traits for storage/indexing/access control.
- `packages/treecrdt-sqlite-ext`: SQLite/wa-sqlite extension harness that will implement the core traits.
- `packages/treecrdt-wasm`: bridge for wasm-bindgen and browser/node builds.
- `packages/treecrdt-wasm-js`: TS/JS wrapper for the treecrdt-wasm build
- `packages/treecrdt-ts`: TypeScript interface definitions shared by bindings and the sync layer.
- `packages/treecrdt-sqlite-node`: TreeCRDT SQLite client for Node.js use
- `packages/treecrdt-wa-sqlite`: TreeCRDT SQLite client for browser use
- `packages/treecrdt-benchmark`: Benchmark utilities
- `packages/discovery`: bootstrap contract for resolving docs to attachment plans
- `packages/treecrdt-sync`: high-level **client** sync for WebSocket + discovery + SQLite `SyncBackend` (npm: `@treecrdt/sync`). Builds on `@treecrdt/sync-protocol` and `@treecrdt/discovery`.
- `packages/sync-protocol/protocol`: sync protocol/runtime core, transport-agnostic (npm: `@treecrdt/sync-protocol`)
- `packages/sync-protocol/material/sqlite`: SQLite-backed sync adapters and proof-material stores
- `packages/sync-protocol/material/postgres`: Postgres-backed sync proof-material stores
- `packages/sync-protocol/server/core`: shared WebSocket sync server runtime
- `packages/sync-protocol/server/postgres-node`: Node sync server wired to Postgres backend

## Quick start

```
pnpm install
pnpm build
pnpm test
```

## Benchmarks

For benchmark commands, product-facing note/sync scenarios, and the sync target matrix (`direct`, local Postgres sync server, remote sync server), see [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## SQLite Clients

TreeCRDT intentionally keeps separate SQLite app entrypoints for Node and browser runtimes:
`@treecrdt/sqlite-node` for `better-sqlite3`/native extension usage, and
`@treecrdt/wa-sqlite` for browser wa-sqlite/WASM usage. See
[docs/SQLITE_CLIENTS.md](docs/SQLITE_CLIENTS.md) for the import-surface decision and examples.

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

- `packages/sync-protocol/server/postgres-node/README.md`
- `examples/playground/README.md`

## Contribute

Contributions are welcome!
