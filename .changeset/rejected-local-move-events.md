---
'@treecrdt/wa-sqlite': patch
---

Report no change for a local move into the node's own subtree. Moving a node under itself or one of its descendants leaves the tree unchanged, but `onMaterialized` reported the move as done, so anything following the events placed the node under its new parent. The op is still recorded, as before.
