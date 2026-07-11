---
'@treecrdt/auth': patch
---

Fail closed for ancestry-scoped operation writes until operations carry a verifiable causal ancestry witness. Stateful subtree scopes continue to authorize reads; doc-wide write grants keep their existing fast path. Previously pending scoped writes are denied, and upgraded peers reject already-applied scoped writes when they are synced, so deployments using scoped write tokens require a coordinated migration.
