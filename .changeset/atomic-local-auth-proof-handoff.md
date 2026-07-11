---
'@treecrdt/auth': patch
'@treecrdt/interface': patch
'@treecrdt/sync-postgres': patch
'@treecrdt/sync-protocol': patch
'@treecrdt/sync-sqlite': patch
---

Return verified, backend-neutral local operation proofs directly from `authorizeLocalOps` so storage adapters can commit proof rows atomically with their operations.

An auth entry is now one complete, durable unit: its 64-byte signature binds the selected 16-byte `proofRef`, and verifiers resolve only that referenced capability. Outbound sync reuses an exact retained proof and never mints auth after the local write has already committed.
