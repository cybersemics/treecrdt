---
'@treecrdt/wa-sqlite': minor
'@treecrdt/wasm': minor
---

Add create-only inserts: an insert with `if_absent` set does nothing where its node is already in the tree at that point in the operation order, so an automatic write never moves a node someone placed. The SQLite extension stores the flag in a new `ops.if_absent` column, takes it on appends and writes it on op reads, and adds `treecrdt_local_insert_if_absent`, which takes the args of `treecrdt_local_insert` and returns NULL without writing anything when the node exists.

Catch-up no longer reports a replayed insert of a node it already showed as a second insert: it reports the move from where the node was shown, or nothing, plus the payload when a voided create leaves an older one.
