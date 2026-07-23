---
"@treecrdt/interface": patch
"@treecrdt/wa-sqlite": patch
---

Make `children(parent)` operation filters dependency-closed and omit rejected structural changes so filtered replay converges with full replay.
