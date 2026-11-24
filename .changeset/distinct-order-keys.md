---
'@treecrdt/wasm': patch
'@treecrdt/wa-sqlite': patch
---

Sibling order keys now end with the node id, so two replicas that place nodes in the same gap at the same time never get equal keys. Previously they often did, and a later insert after the first of two tied siblings landed after both. Keys are 18 bytes longer; existing keys stay valid.
