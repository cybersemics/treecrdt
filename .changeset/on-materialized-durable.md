---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
---

Add `onMaterializedDurable(name, listener, { version, reset, limit, onError })` to the SQLite-backed engines. It is `onMaterialized` that keeps its place: the listener receives every change in order and at least once, across restarts and failed deliveries, so applications no longer store a `changedSince` cursor or write the catch-up loop themselves. The position is kept per name in a new `treecrdt_materialization_cursors` table. After a gap, changes report each node's current state. The returned subscription has `settled()` and `close()`, and closing the client closes open subscriptions.
