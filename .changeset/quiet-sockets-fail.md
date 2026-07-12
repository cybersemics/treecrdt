---
'@treecrdt/sync-protocol': patch
'@treecrdt/sync-server-core': patch
'@treecrdt/sync': patch
---

Add optional transport terminal/close hooks, propagate malformed frames and WebSocket termination through sync sessions instead of leaving pending work unresolved, and scope protocol session routing to the originating transport.
