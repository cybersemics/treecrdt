# TreeCRDT playground (Vite + React)

A small, self-contained demo that exercises the `@treecrdt/wa-sqlite` adapter inside a Vite + React + Tailwind UI. It runs the TreeCRDT SQLite extension in wa-sqlite and lets you insert, move, and delete nodes in an expandable tree while watching the underlying operation log.

## Features
- Insert children under any node, reorder siblings (up/down), move nodes back to the root, or delete them (root is protected).
- Collapsible tree with per-node controls and a composer form to target any parent.
- Live CRDT operation log with lamport/counter metadata.
- Labels are stored in `localStorage`; structure lives in an in-memory wa-sqlite DB.
- Draft sync UI (v0) using `@treecrdt/sync` over `BroadcastChannel` (same-origin).

## Running locally
```bash
pnpm install --filter @treecrdt/playground
pnpm -C examples/playground dev
```

## Sync (v0 draft)

The playground includes a simple sync panel that discovers other open tabs via `BroadcastChannel`.

- Open tab A with a chosen doc and replica: `http://localhost:5173/?doc=demo&replica=replica-a`
- Open tab B with the same doc and a different replica: `http://localhost:5173/?doc=demo&replica=replica-b`
- Make changes in either tab, then click `Sync all` (or `Sync children`) to reconcile and exchange missing ops.

`predev`/`prebuild` copy the wa-sqlite artifacts from `vendor/wa-sqlite/dist` into `public/wa-sqlite`. If that folder is missing, run `make dist` inside `vendor/wa-sqlite` first (the repo already ships a built `dist/`).

The example does not depend on the npm `wa-sqlite` package; it consumes the repo's checked-in wa-sqlite build directly via the copy step above.

## Building / deploying to GitHub Pages
```bash
pnpm -C examples/playground build     # outputs to dist/
pnpm -C examples/playground deploy    # pushes dist/ via gh-pages
```

`vite.config.ts` uses `base: "./"` so the built site works from a subpath (GitHub Pages). If your repo is served from a custom base, adjust `base` accordingly.
