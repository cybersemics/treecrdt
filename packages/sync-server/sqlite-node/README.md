# TreeCRDT Sync Server (Node, SQLite)

Minimal TreeCRDT v0 sync server for local development.

## Setup

From the repo root:

1) Install deps:

```sh
pnpm install
```

2) Build the native SQLite extension bundle for Node (needed by `@treecrdt/sqlite-node`):

```sh
pnpm -C packages/treecrdt-sync build
pnpm -C packages/treecrdt-sqlite-node build
```

## Run

From the repo root:

```sh
pnpm sync-server
```

Or run directly:

```sh
pnpm --filter @treecrdt/sync-server-sqlite-node dev
```

Environment variables:

- `PORT` (default: `8787`)
- `HOST` (default: `0.0.0.0`)
- `TREECRDT_DB_DIR` (default: `./data` relative to the current working directory)
- `TREECRDT_IDLE_CLOSE_MS` (default: `30000`)
- `TREECRDT_MAX_PAYLOAD_BYTES` (default: `10485760`)

Health check:

- `GET http://localhost:8787/health`

WebSocket endpoint:

- `ws://localhost:8787/sync?docId=YOUR_DOC_ID`

## Tests

E2E:

```sh
pnpm -C packages/sync-server/sqlite-node test:e2e
```

Notes:

- One `docId` per WebSocket connection.
- Storage is one SQLite file per `docId` under `TREECRDT_DB_DIR`.
- The database must keep a stable `treecrdt_set_doc_id(docId)` value for opRef hashing; this server enforces it.
