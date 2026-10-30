---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
---

Add `tree.changedSince(since, { limit, payloads })` to the SQLite-backed engines. It returns each node whose visible state changed after a `headSeq`, with its current state and the read's `headSeq`, so applications can keep derived data in step across restarts without rebuilding it. Materialization records each changed node's `headSeq` in a new `tree_changes` table within the same commit; `since = 0` reads every node. Existing databases gain the table on open. Catch-up reads scan that table, which keeps recording cheap; follow `onMaterialized` between them.
