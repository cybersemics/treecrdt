---
'@treecrdt/auth': patch
---

Keep local operation authorization side-effect-free by deferring proof-store publication from
`verifyOps` to `onVerifiedOps`. Remote sync still invokes `onVerifiedOps` before backend apply,
preserving proof-first ingestion, while optimistic local backends can verify proposals without
publishing proof material for a write that has not committed.
