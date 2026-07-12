---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
'@treecrdt/sync-sqlite': patch
---

Let dedicated-worker clients prioritize engine reads between background sync append batches while
preserving normal call ordering and bounding foreground starvation.
