---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
---

Send SQLite operation batches as CBOR blobs instead of JSON byte arrays. Retain JSON input for direct SQL callers; stored operations and sync formats are unchanged.
