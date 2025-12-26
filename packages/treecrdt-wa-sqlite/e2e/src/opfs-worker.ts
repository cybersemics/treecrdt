/// <reference lib="webworker" />

import { buildWorkloads, runWorkloads, type BenchmarkResult, type WorkloadName } from "@treecrdt/benchmark";
import { createTreecrdtClient, type TreecrdtClient } from "@treecrdt/wa-sqlite/client";
import type { TreecrdtAdapter } from "@treecrdt/interface";

type StorageKind = "browser-opfs-coop-sync" | "browser-memory";

type WorkerRequest = {
  type: "run";
  storage?: StorageKind;
  sizes?: number[];
  workloads?: WorkloadName[];
  baseUrl?: string;
};

type WorkerResponse =
  | { ok: true; results: BenchPayload[] }
  | { ok: false; error: string };

type BenchPayload = BenchmarkResult & {
  implementation: string;
  storage: StorageKind;
  workload: string;
  extra?: Record<string, unknown>;
};

const defaultSizes = [1, 10, 100, 1_000];
const defaultWorkloads: WorkloadName[] = ["insert-move", "insert-chain", "replay-log"];

async function createAdapter(
  storage: StorageKind,
  baseUrl?: string
): Promise<TreecrdtAdapter & { close: () => Promise<void> }> {
  const clientStorage = storage === "browser-opfs-coop-sync" ? "opfs" : "memory";
  let client: TreecrdtClient | null = null;
  const effectiveBase =
    baseUrl ??
    (typeof self !== "undefined" && "location" in self ? new URL("/", (self as any).location.href).href : "/");
  const filename = clientStorage === "opfs" ? `/bench-${crypto.randomUUID()}.db` : undefined;
  const docId = `bench-${crypto.randomUUID()}`;
  try {
    console.info(`[opfs-worker] creating client storage=${clientStorage} base=${effectiveBase}`);
    client = await createTreecrdtClient({ storage: clientStorage, baseUrl: effectiveBase, filename, docId });
    // sanity check to ensure DB is valid
    await client.ops.all();
  } catch (err) {
    if (client?.close) {
      await client.close();
    }
    const reason = err instanceof Error ? err.message : String(err);
    console.error(`createAdapter failed (${clientStorage}) base=${effectiveBase}:`, err);
    throw new Error(
      JSON.stringify({
        where: "createAdapter",
        storage: clientStorage,
        base: effectiveBase,
        message: reason,
      })
    );
  }
  return {
    appendOp: (op, serializeNodeId, serializeReplica) =>
      client.ops.append({
        ...op,
        meta: {
          ...op.meta,
          id: {
            replica: serializeReplica(op.meta.id.replica),
            counter: op.meta.id.counter,
          },
        },
      }),
    appendOps: async (ops, serializeNodeId, serializeReplica) => {
      await client.ops.appendMany(
        ops.map((op) => ({
          ...op,
          meta: { ...op.meta, id: { replica: serializeReplica(op.meta.id.replica), counter: op.meta.id.counter } },
        }))
      );
    },
    opsSince: async (lamport, root) => client.ops.since(lamport, root),
    close: async () => client.close(),
  };
}

async function runWaSqliteBenchInWorker(
  storage: StorageKind,
  baseUrl: string | undefined,
  sizes: number[] = defaultSizes,
  workloads: WorkloadName[] = defaultWorkloads
): Promise<BenchPayload[]> {
  const workloadDefs = buildWorkloads(workloads, sizes);
  const results: BenchPayload[] = [];

  for (const workload of workloadDefs) {
    console.info(`[opfs-worker] workload ${workload.name} start`);
    // Per-workload isolation: open a fresh adapter, run the workload, then close.
    const adapter = await createAdapter(storage, baseUrl);
    try {
      // Warm-up to reduce first-write effects: a tiny opsSince(0).
      await adapter.opsSince(0);
      const res = await runWorkloads(async () => adapter, [workload]);
      const [result] = res;
      const mergedExtra =
        result.extra && workload.totalOps
          ? { ...result.extra, count: workload.totalOps }
          : result.extra ?? (workload.totalOps ? { count: workload.totalOps } : undefined);
      results.push({
        ...result,
        implementation: "wa-sqlite",
        storage,
        workload: workload.name,
        extra: mergedExtra,
      });
      console.info(`[opfs-worker] workload ${workload.name} done`);
    } catch (err) {
      console.error(`[opfs-worker] workload ${workload.name} failed`, err);
      if (adapter.close) {
        try {
          await adapter.close();
        } catch {
          // ignore cleanup errors on failure
        }
      }
      throw err;
    }
  }

  return results;
}

self.onmessage = async (ev: MessageEvent<WorkerRequest>) => {
  if (ev.data?.type !== "run") return;
  const storage: StorageKind = ev.data.storage ?? "browser-opfs-coop-sync";
  try {
    const results = await runWaSqliteBenchInWorker(storage, ev.data.baseUrl, ev.data.sizes, ev.data.workloads);
    const response: WorkerResponse = { ok: true, results };
    self.postMessage(response);
  } catch (err) {
    console.error("[opfs-worker] bench failed", err);
    const response: WorkerResponse = { ok: false, error: err instanceof Error ? err.message : String(err) };
    self.postMessage(response);
  }
};
