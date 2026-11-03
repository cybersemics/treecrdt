---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
---

Add `tree.node(id)`, a node handle that skips the existence query `tree.get(id)` runs, so reading a known node by id is one query: `await tree.node(id).children()`. A node that is not visible reads as empty: no payload, no children and no parent. Adapter `treePayload` and `treeChildren` now return nothing for a node that is not visible, instead of a deleted node's kept payload or hidden children.
