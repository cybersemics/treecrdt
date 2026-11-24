---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
'@treecrdt/wasm': minor
'@treecrdt/sync-protocol': minor
'@treecrdt/auth': minor
'@treecrdt/sync-sqlite': patch
---

Add create-only inserts: an insert with `ifAbsent` set does nothing where its node is already in the tree at that point in the operation order, so an automatic write never moves a node someone placed. `local.insertIfAbsent` writes one, or resolves to null and writes nothing when the node is already in the tree. The SQLite extension stores the flag in a new `ops.if_absent` column, so recreate databases made by earlier versions, and adds `treecrdt_local_insert_if_absent`, which takes the args of `treecrdt_local_insert` and returns NULL without writing anything when the node exists. Sync v0 carries the flag as `InsertOp.if_absent`, and op-sig/v1 now signs it for every insert: recreate development documents and stored op auth signed with the earlier format.

Catch-up no longer reports a replayed insert of a node it already showed as a second insert: it reports the move from where the node was shown, or nothing, plus the payload when a voided create leaves an older one. `children(parent)` sync no longer lists a create-only insert that changed nothing as the parent's payload writer.
