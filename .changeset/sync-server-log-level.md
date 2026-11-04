---
'@treecrdt/sync-server-core': patch
'@treecrdt/sync-server-postgres-node': patch
---

Add pino logs to the sync server. `TREECRDT_LOG_LEVEL` selects `debug`, `info`, `warn`, `error`, or `silent`. When it is unset, `NODE_ENV=production` uses `info` and other environments use `debug`. Debug logs include each sync message the server sends or receives.
