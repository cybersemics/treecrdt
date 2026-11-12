---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
'@treecrdt/sync-sqlite': minor
---

Add `run(sql, params)` to `SqliteRunner`. It runs one statement with bound parameters to completion and returns nothing, so parameterized writes no longer need `getText` with `RETURNING 1` appended. `client.runner` implements it, and TreeCRDT's own writes (`onMaterializedDurable` positions and the sync-sqlite stores) now use it. Custom runners must implement `run`.
