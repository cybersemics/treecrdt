---
'@treecrdt/wa-sqlite': patch
---

Close failed SQLite and OPFS resources, retry memory fallback with a fresh module, and release SharedWorker ports when initialization or teardown fails.
