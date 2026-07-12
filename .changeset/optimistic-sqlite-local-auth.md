---
'@treecrdt/interface': patch
'@treecrdt/wa-sqlite': patch
---

Authorize SQLite local operations from a read-only proposal, then acquire the write lock and
atomically revalidate the clean materialization revision and exact operation before committing.
Concurrent writes now trigger bounded re-authorization instead of entering an auth savepoint, so
an auth rejection cannot roll back unrelated work on the same connection.

The verified local op proof returned by authorization is stored in the standard SQLite op-auth
sidecar atomically with the operation and materialization. A process crash can no longer leave a
committed scoped operation without its original proof. The native row is authoritative, so
materialization events are emitted as soon as that atomic commit succeeds.
