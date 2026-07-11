---
'@treecrdt/auth': patch
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
---

Authorize structural writes against both their destination and the node's stable pre-write ancestry. SQLite local writes now carry explicit pre-write node state into auth, existing-node insert upserts cannot pull nodes across a subtree boundary, and genuinely new inserts remain supported.
