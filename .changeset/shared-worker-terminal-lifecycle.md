---
'@treecrdt/wa-sqlite': patch
---

Invalidate every shared-worker client when one client drops the shared database, and prune stale ports before resetting the worker session.
