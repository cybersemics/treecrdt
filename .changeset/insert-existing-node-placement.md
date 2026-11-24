---
'@treecrdt/wa-sqlite': patch
---

Place and index a local insert of an existing node the way a move would, as happens when two writers create the same deterministic id. With `last` the insert failed when the node was already the last child, and under another parent the old parent's `opRefs.children` missed the insert and the new parent's missed the node's payload.
