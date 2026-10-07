---
'@treecrdt/wa-sqlite': minor
---

Initialize the shared TreeCRDT schema through awaited SQL in one transaction,
replacing the custom VFS retry loop. Failed initialization rolls back schema changes.
Document ID setup shares the write transaction so concurrent opens cannot race.

Low-level callers must now pass the connection's SQL runner as the third argument
to `initializeTreecrdtExtension(module, handle, db, docId)`, with an optional document
ID to initialize in the same transaction. Rebuild the vendor WASM and
adapter together. `createTreecrdtClient()` callers need no changes.
