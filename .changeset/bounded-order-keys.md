---
'@treecrdt/wasm': patch
'@treecrdt/wa-sqlite': patch
---

Keep sibling order keys short for appends, prepends, and repeated inserts at one position. Previously each append added two bytes, so a 2,000-item list produced 4 KB keys. Existing keys stay valid, and appends after them are short again.
