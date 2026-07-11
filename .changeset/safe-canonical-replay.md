---
"@treecrdt/interface": patch
"@treecrdt/wa-sqlite": patch
---

Rebuild derived state from canonical replay, remove the causally unsafe payload no-op shortcut, preserve zero-length payloads distinctly from null, roll failed appends back atomically, validate portable operation keys, and reject malformed cycle or sentinel structural changes without emitting false events.
