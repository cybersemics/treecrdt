---
'@treecrdt/wa-sqlite': patch
---

Report a node as deleted again when a late op rejects the move that revived it. An out-of-order op that turned an already-applied move into a cycle left the revived node out of `onMaterialized`, `onMaterializedDurable` and `changedSince`, so listeners kept showing it while tree reads hid it.
