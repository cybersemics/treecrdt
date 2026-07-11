---
'@treecrdt/sync-protocol': patch
'@treecrdt/sync-server-core': patch
'@treecrdt/sync': patch
---

Add optional transport terminal/close hooks and propagate malformed frames and WebSocket termination through sync sessions instead of leaving pending work unresolved.
