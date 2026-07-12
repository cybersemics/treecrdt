---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
'@treecrdt/sync-sqlite': patch
---

Let worker-backed clients prioritize reads between background sync append batches while preserving
normal call ordering and bounding foreground starvation.
