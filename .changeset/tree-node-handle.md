---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
---

Add `tree.node(id)`, a node handle that skips the existence query `tree.get(id)` runs, so reading a known node by id is one query: `await tree.node(id).children()`. Node reads now check visibility themselves, so a node that is not visible reads as empty: no payload, no children and no parent. Before, only `get()` checked, and a handle kept across a delete still read the deleted node's payload and children; so did adapter `treePayload` and `treeChildren`.
