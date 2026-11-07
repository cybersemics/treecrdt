---
'@treecrdt/sync-server-core': patch
'@treecrdt/sync-server-postgres-node': patch
---

Add leveled sync-server logs. `TREECRDT_LOG_LEVEL` selects `debug`, `info`, `warn`, `error`, or `silent`, and defaults to `info` even when `NODE_ENV` is unset. JSON is the default format; `TREECRDT_LOG_PRETTY=true` opts into pretty lines. Debug logs include each sync message the server sends or receives. A failed Postgres LISTEN connection exits the CLI, and a clean WebSocket close is not reported as a handler error.
