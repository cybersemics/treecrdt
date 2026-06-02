---
'@treecrdt/sync': minor
'@treecrdt/sync-protocol': patch
---

Add an outbound sync helper for queued local-op upload to remote peer transports. Standard
TreeCRDT operations are deduped by operation id by default; custom op shapes may provide `opKey`.
`SyncPeer` also derives op refs for standard TreeCRDT operations by default, so apps only need
`deriveOpRef` for custom op-ref schemes or nonstandard op shapes.
