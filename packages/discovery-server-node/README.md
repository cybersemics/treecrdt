# TreeCRDT Discovery Server (Node)

Small standalone HTTP bootstrap service for `resolveDoc`.

It is intentionally separate from the websocket sync server:

- discovery/bootstrap happens once at connect time
- clients cache the returned attachment plan
- steady-state sync then talks directly to the resolved `ws://` or `wss://` endpoint

## Run locally

From the repo root:

```sh
pnpm discovery-server:local
```

This starts a bootstrap server on `http://localhost:8788` that advertises:

- bootstrap base: `http://localhost:8788`
- sync websocket base: `ws://localhost:8787`

## Environment

- `HOST` (default: `0.0.0.0`)
- `PORT` (default: `8788`)
- `TREECRDT_DISCOVERY_RESOLVE_PATH` (default: `/resolve-doc`)
- `TREECRDT_DISCOVERY_PUBLIC_HTTP_BASE_URL` (optional absolute HTTP base URL advertised to clients)
- `TREECRDT_DISCOVERY_PUBLIC_WS_BASE_URL` (optional absolute websocket base URL advertised to clients)
- `TREECRDT_DISCOVERY_CACHE_TTL_MS` (default: `3600000`)

## Endpoints

- `GET /health`
- `GET /status`
- `GET /resolve-doc?docId=YOUR_DOC_ID`
