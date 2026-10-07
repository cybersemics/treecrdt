---
'@treecrdt/wa-sqlite': minor
---

Replace the custom worker RPC with Comlink and split session data access from connection lifecycle. `createWaSqliteApi` is removed; low-level callers that open a wa-sqlite handle themselves should initialize the extension, set the doc id, then pass the database to `createTreecrdtSqliteAdapter` from `@treecrdt/interface/sqlite`. `createTreecrdtClient()` is unchanged for typical apps.
