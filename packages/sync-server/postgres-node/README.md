# TreeCRDT Sync Server (Node, Postgres)

Minimal TreeCRDT v0 sync server backed by a Postgres backend module.

## Setup

From the repo root:

```sh
pnpm install
pnpm -C packages/treecrdt-sync run build
pnpm --filter @treecrdt/sync-server-core build
pnpm -C packages/sync-server/postgres-node run build
```

## Run

From the repo root:

```sh
TREECRDT_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
pnpm --filter @treecrdt/sync-server-postgres-node dev
```

Environment variables:

- `TREECRDT_POSTGRES_URL` (required)
- `TREECRDT_POSTGRES_BACKEND_MODULE` (default: `./packages/treecrdt-postgres-napi/dist/index.js`)
- `PORT` (default: `8787`)
- `HOST` (default: `0.0.0.0`)
- `TREECRDT_IDLE_CLOSE_MS` (default: `30000`)
- `TREECRDT_MAX_PAYLOAD_BYTES` (default: `10485760`)

Health check:

- `GET http://localhost:8787/health`

WebSocket endpoint:

- `ws://localhost:8787/sync?docId=YOUR_DOC_ID`

## Notes

- One `docId` per WebSocket connection.
- The backend module must export `createPostgresNapiSyncBackendFactory(url)`.
