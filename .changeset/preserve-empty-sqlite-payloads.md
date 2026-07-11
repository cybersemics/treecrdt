---
"@treecrdt/interface": patch
"@treecrdt/wa-sqlite": patch
---

Preserve zero-length SQLite payloads distinctly from null across local writes, operation reads, replay, and reopen. Keep exact-state version-vector buffers alive until SQLite consumes them so a later local write cannot observe corrupted replay metadata.
