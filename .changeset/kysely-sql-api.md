---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
'@treecrdt/auth': minor
'@treecrdt/sync-sqlite': minor
---

Add a Kysely-backed type-safe SQLite API (`createTreecrdtDb` / `client.db`) and remove the raw `runner` SQL surface from clients. Auth/sync helpers now take `db` instead of `SqliteRunner`. Transactions are unsupported on this dialect.
