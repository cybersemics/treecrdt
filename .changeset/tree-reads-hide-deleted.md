---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
---

Stop node reads from returning a deleted node's data. Only `tree.get(id)` checked visibility, so a handle kept across a delete still read the deleted node's payload and children, and so did adapter `treePayload` and `treeChildren`. They now check it in the same query.
