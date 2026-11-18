---
'@treecrdt/interface': minor
'@treecrdt/wa-sqlite': minor
---

Add `idle()` to the subscription that `onMaterializedDurable` returns. It resolves once the deliveries queued before the call have finished, whether or not they succeeded. Unlike `settled()`, it never starts a catch-up and never rejects, so a caller can wait for a listener that keeps failing without retrying it.
