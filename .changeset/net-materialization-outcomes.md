---
"@treecrdt/interface": patch
"@treecrdt/wa-sqlite": patch
---

Emit only the net node-backed changes produced by canonical catch-up. Replay transitions that cancel before the final state no longer trigger materialization events, and catch-up-derived changes may omit operation provenance. Failed incremental writes are rolled back before canonical repair so catch-up outcomes retain exact before/after semantics.
