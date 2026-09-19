---
'@treecrdt/wa-sqlite': major
---

Reduce `createTreecrdtClient()` options to a required `docId` plus an optional `persistent` flag. The public `storage` / `runtime` / `assets` / `filename` configuration and the `TreecrdtStorage`, `TreecrdtRuntime`, and `TreecrdtAssets` types are removed. The package now selects everything internally: in-memory + direct runtime by default, and OPFS + dedicated worker for `persistent: true` with the filename derived deterministically from `docId` (exported as `opfsFilenameForDocId`). The previous `auto` default (OPFS when available) becomes in-memory; persistence is always explicit and never silently falls back — when OPFS is unavailable or fails to initialize, client creation throws with the reason. On Node, `persistent: true` throws; use `@treecrdt/sqlite-node` for file persistence.
