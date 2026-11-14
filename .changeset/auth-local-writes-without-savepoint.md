---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
---

Stop holding a SQLite savepoint while `authorizeLocalOps` runs. Auth-checked local writes now authorize the op before it is applied, as peers do, and write it only if no other op was applied in between; otherwise the op is minted and authorized again, up to four times. A denied write no longer rolls back writes made meanwhile on the same connection, including other `crossTab` tabs, and concurrent auth-checked writes of a replica take turns instead of failing after committing. Update the adapter and extension together.
