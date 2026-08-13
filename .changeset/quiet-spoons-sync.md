---
'@treecrdt/wa-sqlite': patch
---

Use the synchronous wa-sqlite build for dedicated-worker OPFS and memory databases while retaining
the Asyncify build for direct and shared-worker OPFS access.
