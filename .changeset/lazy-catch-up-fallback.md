---
'@treecrdt/wa-sqlite': patch
---

Stop replaying the whole op log when an out-of-order op lands before nothing but already-applied payload ops. The direct rewind already handled that append, but the full catch-up still ran after it and its result was discarded, so these appends got slower as the document grew.
