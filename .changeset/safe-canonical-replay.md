---
"@treecrdt/wa-sqlite": patch
---

Rebuild derived state from canonical replay, roll failed appends back atomically, validate portable operation keys, and reject malformed cycle or sentinel structural changes without emitting false events.
