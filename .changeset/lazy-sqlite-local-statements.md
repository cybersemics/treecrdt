---
'@treecrdt/wa-sqlite': patch
---

Reduce local insert, move, delete, and payload overhead by preparing SQLite helper statements only when each operation needs them.
