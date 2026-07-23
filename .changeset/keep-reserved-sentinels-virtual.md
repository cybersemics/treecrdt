---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
---

Keep the reserved TRASH node virtual across in-memory, SQLite, and PostgreSQL materialization.
Operations that target TRASH, and structural operations that target ROOT, now remain deterministic
no-ops without creating sentinel rows, payloads, or materialization events. ROOT payload operations
remain supported.
