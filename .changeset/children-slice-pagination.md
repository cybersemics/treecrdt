---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
---

Replace keyset `treeChildrenPage` with optional index/length slices on `treeChildren` and `TreecrdtNode.children()`. Live (non-tombstoned) children are paginated in stable order.
