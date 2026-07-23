---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
---

Reject operation-id equivocation at memory, SQLite, and PostgreSQL storage boundaries. Exact duplicate operations remain idempotent, while reusing a replica/counter id with different contents now fails without committing any part of the batch.

SQLite and wa-sqlite `appendMany` calls now use one atomic bulk-extension call. The generic per-operation retry and cross-worker chunking have been removed because they could commit a prefix before returning a validation error. As before, the SQLite adapter requires the matching extension with `treecrdt_append_ops`; the old-extension compatibility fallback had already stopped working once it was changed to retry through that same function.

This closes silent divergence when conflicting operation bodies reach the same storage boundary. A future content-bound op-ref protocol version is still needed to detect Byzantine peers that never exchange both conflicting bodies.
