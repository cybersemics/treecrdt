---
"@treecrdt/wasm": patch
---

Add client-owned transactional materialization observers with checked read barriers and rollback recovery. Remove the public transaction getChanges method; committed transactions still return their final changes.
