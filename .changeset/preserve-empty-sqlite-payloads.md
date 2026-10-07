---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
---

Preserve zero-length SQLite payloads and order keys distinctly from null across local writes, remote appends, operation reads, replay, and reopen. Batch appends now reject empty replica IDs instead of storing operations that later fail to read.
