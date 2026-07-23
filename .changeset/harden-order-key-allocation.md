---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
---

Generate deterministic sibling order keys with seed entropy so concurrent local inserts no longer collapse into a tiny set of positions. Structural keys use one extensible encoding and are always strictly inside their requested bounds.

Exact `After(node)` placement now fails when a following sibling has the same order key, because that position cannot be represented without rekeying. Moves to Trash retain their empty sentinel key.
