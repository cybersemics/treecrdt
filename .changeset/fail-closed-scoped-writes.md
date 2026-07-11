---
'@treecrdt/auth': patch
---

Require document-wide grants for operation writes because receiver-local ancestry is not a stable authorization witness. Stateful subtree scopes remain available for reads, and document-wide writes retain their direct path.
