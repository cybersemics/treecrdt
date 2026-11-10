---
'@treecrdt/wa-sqlite': patch
---

`ops.appendMany` with more than 2,500 operations now emits one materialization event per chunk, when that chunk commits, instead of one merged event after the last chunk. Events stay in commit order when another write lands between two chunks, and the changes of committed chunks are still reported if a later chunk fails.
