---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
---

Add placement `{ type: 'between', left, right }` to local inserts and moves on the SQLite-backed engines. It places a node at a new order key strictly between two keys, which need not be its siblings' keys (null is an open end), and the key ends with the node id, so concurrent placements into one gap never share a key.
