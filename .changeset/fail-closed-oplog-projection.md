---
'@treecrdt/auth': patch
---

Require document-wide `read_structure` for reference COSE/CWT operation-log filters, and document-wide `read_payload` before projecting payload updates, clears, or payload-bearing inserts. This supersedes the current-materialized-membership filtering proposed in #183, which could reveal excluded destinations and private history after a node re-entered readable state.
