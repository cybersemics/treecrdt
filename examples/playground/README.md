# TreeCRDT playground (Vite + React)

A small, self-contained demo that exercises the `@treecrdt/wa-sqlite` adapter inside a Vite + React + Tailwind UI. It runs the TreeCRDT SQLite extension in wa-sqlite and lets you insert, move, and delete nodes in an expandable tree while watching the underlying operation log.

## Features
- Insert children under any node, reorder siblings (up/down), move nodes back to the root, or delete them (root is protected).
- Collapsible tree with per-node controls and a composer form to target any parent.
- Live CRDT operation log with lamport/counter metadata.
- Labels are stored in `localStorage`; structure lives in an in-memory wa-sqlite DB.
- Draft sync UI (v0) using `@treecrdt/sync` over `BroadcastChannel` (same-origin).
- Optional auth/ACL demo (COSE_Sign1 + CWT subtree capabilities) with invite links, per-op signatures, and a pending-op inspector.

## Running locally
```bash
pnpm install --filter @treecrdt/playground
pnpm -C examples/playground dev
```

## Quick start with a real sync server

If you want to test the playground against a real local sync server, use this flow first:

```bash
# Build all workspace packages, including the Postgres sync-server path.
pnpm build
# Start a disposable local Postgres instance in Docker on localhost:5432.
pnpm sync-server:postgres:db:start
# Start the TreeCRDT sync server on ws://localhost:8787 using that Postgres DB.
pnpm sync-server:postgres:local
# Start the playground UI.
pnpm -C examples/playground dev
```

Then in the playground:

- Open the `Connections` panel
- Paste `ws://localhost:8787` into `Remote sync server`
- Leave mode as `Hybrid`, or switch to `Remote server` if you want to disable local tab sync

## Public sync server

If you want to test against the shared dev deployment instead of running Postgres locally:

- Open the `Connections` panel
- Click `Use public`, or paste `wss://sync.emhub.net/sync`
- Use `Hybrid` for browser-local tabs plus remote sync, or `Remote server` for remote-only behavior

The public hostname is TLS-enabled, so use `wss://`, not `ws://`.

`pnpm sync-server:postgres:db:start` starts a disposable local Postgres at:

```bash
postgres://postgres:postgres@127.0.0.1:5432/postgres
```

Stop it later with:

```bash
pnpm sync-server:postgres:db:stop
```

## Sync (v0 draft)

The playground includes a `Connections` panel with three sync modes:

- `Local tabs`: sync same-origin tabs via `BroadcastChannel`
- `Remote server`: sync only via websocket
- `Hybrid`: use both local tabs and websocket

For local tab sync:

- Open tab A with a chosen doc: `http://localhost:5167/?doc=demo`
- Open tab B with the same doc: `http://localhost:5167/?doc=demo` (each tab gets its own replica key)
- Make changes in either tab, then click `Sync all` (or `Sync children`) to reconcile and exchange missing ops.

### Auth / ACL demo

Open the `Auth` panel (key icon) and:

- Enable auth to bootstrap an issuer + a full-access token for the current tab (stored in `sessionStorage` for this tab only).
- Generate an invite link for a subtree (root + actions + optional depth) and open it in a new tab.
- The invited tab can only write within the granted scope; out-of-scope writes are rejected (fail-closed).
- If ops arrive out-of-order and scope can’t be determined yet, they show up as “pending ops” until ancestry context arrives.

The Vite app uses `@treecrdt/wa-sqlite/vite-plugin` to copy wa-sqlite artifacts from `@treecrdt/wa-sqlite-vendor` into `public/wa-sqlite` on startup.

If you see `SQLiteError: no such function: treecrdt_set_doc_id`, your wa-sqlite build is stale relative to `packages/treecrdt-sqlite-ext`. Rebuild wa-sqlite and reload:

```bash
pnpm --filter @treecrdt/wa-sqlite-vendor rebuild
```

The example does not depend on the npm `wa-sqlite` package; it consumes the repo's git submodule build directly via the copy step above.

## Building / deploying to GitHub Pages
```bash
pnpm -C examples/playground build     # outputs to dist/
pnpm -C examples/playground deploy    # pushes dist/ via gh-pages
```

`vite.config.ts` uses `base: "./"` so the built site works from a subpath (GitHub Pages). If your repo is served from a custom base, adjust `base` accordingly.
