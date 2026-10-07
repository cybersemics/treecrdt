---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
---

Send SQLite operation batches as CBOR blobs instead of JSON byte arrays. Update the adapter and extension together: JSON batch input is no longer accepted. Stored operations and sync formats are unchanged.
