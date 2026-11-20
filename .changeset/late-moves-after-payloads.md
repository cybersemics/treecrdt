---
'@treecrdt/wasm': patch
---

Avoid replaying history for commuting payload/deletion updates and late inserts or moves ordered after prior placements, including equal-timestamp ties.
