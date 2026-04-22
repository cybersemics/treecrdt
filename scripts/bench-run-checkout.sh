#!/usr/bin/env bash
set -euo pipefail

CHECKOUT_DIR=""
BENCH_SUITE=""
TREECRDT_POSTGRES_URL="${TREECRDT_POSTGRES_URL:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --checkout)
      CHECKOUT_DIR="$2"
      shift 2
      ;;
    --suite)
      BENCH_SUITE="$2"
      shift 2
      ;;
    --postgres-url)
      TREECRDT_POSTGRES_URL="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$CHECKOUT_DIR" || -z "$BENCH_SUITE" ]]; then
  echo "Usage: $0 --checkout <dir> --suite <default|sync|hot-write|full> [--postgres-url <url>]" >&2
  exit 1
fi

cd "$CHECKOUT_DIR"

if [[ ! -d node_modules ]]; then
  pnpm install --frozen-lockfile=false
fi

pnpm -C packages/treecrdt-ts run build
pnpm -C packages/treecrdt-riblt-wasm-js run build
pnpm -C packages/sync/protocol run build
pnpm -C packages/sync/material/sqlite run build
pnpm -C packages/sync/material/postgres run build
pnpm -C packages/sync/server/core run build
pnpm -C packages/treecrdt-auth run build
pnpm -C packages/treecrdt-crypto run build

if [[ "$BENCH_SUITE" == "full" ]]; then
  pnpm -C packages/treecrdt-sqlite-conformance run build
  pnpm -C packages/treecrdt-wa-sqlite run build:ts
  pnpm -C packages/treecrdt-wa-sqlite/e2e run build:wa-sqlite
fi

has_hot_write_suite() {
  node -e "const p=require('./package.json'); process.exit(p.scripts?.['benchmark:hot-write'] ? 0 : 1)"
}

run_hot_write_suite() {
  if ! has_hot_write_suite; then
    echo "Skipping hot-write benchmarks (scripts not present on this branch)"
    return 0
  fi

  pnpm -C packages/treecrdt-sqlite-node run benchmark:hot-write -- "$@"

  if [[ -n "$TREECRDT_POSTGRES_URL" ]]; then
    pnpm -C packages/treecrdt-postgres-napi run benchmark:hot-write -- "$@"
  else
    echo "Skipping postgres hot-write benchmark (TREECRDT_POSTGRES_URL not set)"
  fi
}

case "$BENCH_SUITE" in
  default)
    pnpm run benchmark:sync:local -- \
      --workloads=sync-balanced-children-payloads-cold-start \
      --counts=100000 \
      --first-view \
      --iterations=3 \
      --warmup=1 \
      --server-fixture-cache=rebuild
    run_hot_write_suite \
      --benches=payload-edit,insert-sibling \
      --counts=100000 \
      --writes-per-sample=10 \
      --warmup-writes=2
    ;;
  sync)
    pnpm run benchmark:sync:local -- \
      --workloads=sync-balanced-children-cold-start,sync-balanced-children-payloads-cold-start \
      --counts=10000,100000 \
      --first-view \
      --iterations=3 \
      --warmup=1 \
      --server-fixture-cache=rebuild
    ;;
  hot-write)
    run_hot_write_suite \
      --benches=payload-edit,insert-sibling \
      --counts=10000,100000 \
      --writes-per-sample=10 \
      --warmup-writes=2
    ;;
  full)
    pnpm run benchmark
    pnpm run benchmark:sync:local -- \
      --workloads=sync-balanced-children-payloads-cold-start \
      --counts=100000 \
      --first-view \
      --iterations=3 \
      --warmup=1 \
      --server-fixture-cache=rebuild
    run_hot_write_suite \
      --benches=payload-edit,insert-sibling \
      --counts=100000 \
      --writes-per-sample=10 \
      --warmup-writes=2
    ;;
  *)
    echo "Unknown BENCH_SUITE=$BENCH_SUITE" >&2
    exit 1
    ;;
esac

pnpm run benchmark:aggregate
