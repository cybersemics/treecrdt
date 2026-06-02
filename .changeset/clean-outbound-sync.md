---
'@treecrdt/sync': minor
---

Add an outbound sync helper for queued local-op upload to remote peer transports. Standard
TreeCRDT operations are deduped by operation id by default; custom op shapes may provide `opKey`.
