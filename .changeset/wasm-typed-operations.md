---
'@treecrdt/wasm': patch
---

Pass operation objects and binary fields directly into WASM instead of encoding inputs as JSON and hex. Preserve the adapter API, operation-read format, and canonical version-vector bytes.
