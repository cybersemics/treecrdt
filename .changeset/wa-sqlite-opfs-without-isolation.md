---
'@treecrdt/wa-sqlite': patch
---

Allow persistent OPFS storage on pages that are not cross-origin isolated. `detectOpfsSupport()` and `createTreecrdtClient({ persistent: true })` no longer require COOP/COEP headers.
