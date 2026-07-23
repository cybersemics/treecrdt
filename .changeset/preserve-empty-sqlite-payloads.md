---
"@treecrdt/interface": patch
"@treecrdt/wa-sqlite": patch
---

Preserve zero-length SQLite payloads and order keys distinctly from null across local writes, remote appends, operation reads, replay, and reopen. Reject empty replica IDs without aborting or leaving SQLite statements open.
