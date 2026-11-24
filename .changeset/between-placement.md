---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
---

Add placements `{ type: 'between', left, right }` and `{ type: 'at', key }` to local inserts and moves on the SQLite-backed engines. 'between' places a node at a new order key strictly between two keys, which need not be its siblings' keys (null is an open end), and ends the key with the node id, so concurrent placements into one gap never share a key; 'at' places it at exactly the given canonical key, so devices that derive a key the same way write the same placement.
