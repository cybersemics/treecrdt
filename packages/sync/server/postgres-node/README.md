# TreeCRDT Sync Server (Node, Postgres)

Minimal TreeCRDT v0 sync server backed by a Postgres backend module.

## Setup

From the repo root:

```sh
# Install workspace dependencies and create workspace links.
pnpm install
# Build all workspace packages, including the Postgres sync-server path.
pnpm build
```

## Run

From the repo root:

```sh
# Start a disposable local Postgres instance in Docker on localhost:5432.
pnpm sync-server:postgres:db:start
# Start the TreeCRDT sync server on ws://localhost:8787 using that Postgres DB.
pnpm sync-server:postgres:local
```

The local database helper starts a disposable Postgres at:

```sh
postgres://postgres:postgres@127.0.0.1:5432/postgres
```

Stop it later with:

```sh
pnpm sync-server:postgres:db:stop
```

If you want a custom connection string instead:

```sh
TREECRDT_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
pnpm sync-server:postgres
```

Environment variables:

- `TREECRDT_POSTGRES_URL` (required)
- `TREECRDT_POSTGRES_HOST`, `TREECRDT_POSTGRES_PORT`, `TREECRDT_POSTGRES_DB`, `TREECRDT_POSTGRES_USER`, `TREECRDT_POSTGRES_PASSWORD` (optional alternative to `TREECRDT_POSTGRES_URL`)
- `TREECRDT_POSTGRES_BACKEND_MODULE` (default: `./packages/treecrdt-postgres-napi/dist/index.js`)
- `PORT` (default: `8787`)
- `HOST` (default: `0.0.0.0`)
- `TREECRDT_IDLE_CLOSE_MS` (default: `30000`)
- `TREECRDT_MAX_PAYLOAD_BYTES` (default: `10485760`)
- `TREECRDT_SYNC_AUTH_TOKEN` (optional static token, accepted as `Authorization: Bearer ...` or `?token=...`)
- `TREECRDT_SYNC_CWT_ISSUER_PUBKEYS` (optional comma-separated base64url Ed25519 issuer public keys, enables CWT capability auth)
- `TREECRDT_DOC_ID_PATTERN` (optional regex; deny docs that do not match)
- `TREECRDT_ALLOW_DOC_CREATE` (default: `true`; when `false`, unknown `docId` values are denied)
- `TREECRDT_PG_NOTIFY_ENABLED` (default: `true`; enables LISTEN/NOTIFY fanout across server instances)
- `TREECRDT_PG_NOTIFY_CHANNEL` (default: `treecrdt_sync_doc_updates`)
- `TREECRDT_RATE_LIMIT_MAX_UPGRADES` (default: `0`, disabled; per-IP upgrades per window)
- `TREECRDT_RATE_LIMIT_WINDOW_MS` (default: `60000`)

Health check:

- `GET http://localhost:8787/health`

Status endpoint:

- `GET http://localhost:8787/status`
  Returns JSON debug metadata for the running server, including readiness, package version, git SHA when available,
  protocol version, startup time, and key runtime settings.

WebSocket endpoint:

- `ws://localhost:8787/sync?docId=YOUR_DOC_ID`

## Notes

- One `docId` per WebSocket connection.
- The backend module must export `createPostgresNapiSyncBackendFactory(url)`.
- Multi-instance fanout uses Postgres LISTEN/NOTIFY on the configured channel.
