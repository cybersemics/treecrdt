---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
---

Isolate materialization listener failures after a write or recovery has committed. One throwing
listener no longer rejects the completed operation or prevents later listeners from observing it.
