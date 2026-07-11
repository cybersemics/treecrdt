---
'@treecrdt/auth': patch
'@treecrdt/sync-protocol': patch
---

Fail closed on every outbound operation path: validate replacement capability snapshots before
publishing them, serialize fresh Hello/Ack authorization barriers for direct and subscribed pushes,
preflight `all` filters before RIBLT disclosure, and terminate subscriptions after authority loss.
