---
"@treecrdt/wa-sqlite": patch
---

Serialize concurrent SQLite appends before reading materialization metadata so blocked writers cannot regress the canonical head or materialization sequence.
