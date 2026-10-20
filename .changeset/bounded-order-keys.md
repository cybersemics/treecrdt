---
'@treecrdt/wasm': patch
'@treecrdt/wa-sqlite': patch
---

Keep sibling order keys short for appends, prepends, and repeated inserts at one position. Each append previously added two bytes to the new key, so a 2,000-item list produced 4 KB keys. Existing keys stay valid, and new keys allocated after them are short again.
