---
'@treecrdt/sync-protocol': minor
'@treecrdt/sync-server-core': patch
'@treecrdt/sync': minor
---

Require every duplex transport to expose an idempotent `close` method and a read-only `closeSignal`, treat each transport object as one connection lifecycle, propagate malformed frames and WebSocket closure through sync sessions instead of leaving pending work unresolved, and scope protocol session routing to the originating transport. The high-level custom-transport factory now owns transport cleanup and no longer accepts a separate close callback.
