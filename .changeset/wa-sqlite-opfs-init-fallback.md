---
'@treecrdt/wa-sqlite': patch
---

Honor the requested OPFS fallback policy in worker runtimes, close failed SQLite and OPFS resources, retry allowed memory fallback with a fresh module, and release SharedWorker ports when initialization or teardown fails.
