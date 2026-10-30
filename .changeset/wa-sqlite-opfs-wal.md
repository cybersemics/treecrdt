---
'@treecrdt/wa-sqlite': minor
---

Open persistent OPFS databases in WAL mode with exclusive locking, roughly halving the cost of a single-operation commit. A database is now held by one client until it closes: without `crossTab`, another client opening the same database, for example in a second tab, waits until the first client closes or its tab goes away. Use `crossTab: true` to share a persistent database between tabs. Connections that open these files directly must run `PRAGMA locking_mode=EXCLUSIVE` before their first statement.
