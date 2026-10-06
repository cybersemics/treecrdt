---
'@treecrdt/wasm': patch
---

Apply operation batches through one WASM call, replaying retained history at most once. Decode and validate the complete batch before applying any operations.
