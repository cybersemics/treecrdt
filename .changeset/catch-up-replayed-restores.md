---
'@treecrdt/wa-sqlite': patch
---

Keep a node that a catch-up after late ops restores. When the replayed ops deleted a node and then restored it, for example with a peer's payload the delete didn't know about, the node stayed deleted with its old payload until a later catch-up, and neither `onMaterialized` nor `tree.changedSince` reported it. Catch-up now rewrites every node the replay touched.
