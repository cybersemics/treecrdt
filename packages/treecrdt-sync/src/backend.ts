import type { Operation } from '@treecrdt/interface';
import { bytesToHex, hexToBytes } from '@treecrdt/interface/ids';
import type { SqliteRunner } from '@treecrdt/interface/sqlite';

import { deriveOpRefV0 } from './opref.js';
import { createTreecrdtSyncSqlitePendingOpsStore } from './sqlite.js';
import type { Filter, OpRef, SyncBackend } from './types.js';

export type TreecrdtSyncBackendClient = {
  runner?: SqliteRunner;
  meta?: {
    headLamport?: () => Promise<number> | number;
  };
  opRefs: {
    all: () => Promise<OpRef[]>;
    children: (parentHex: string) => Promise<OpRef[]>;
  };
  ops: {
    all?: () => Promise<Operation[]>;
    get: (opRefs: OpRef[]) => Promise<Operation[]>;
    appendMany: (ops: Operation[]) => Promise<void>;
  };
};

async function maybeLoadNodePayloadWriterOpRef(opts: {
  runner: SqliteRunner;
  docId: string;
  nodeBytes: Uint8Array;
}): Promise<Uint8Array | null> {
  // `tree_payload` is a derived table; ensure it is up-to-date before reading.
  try {
    await opts.runner.getText('SELECT treecrdt_ensure_materialized()');
  } catch {
    // Best-effort. Some backends may not expose this UDF (non-SQLite); callers will ignore null.
  }

  // Fast path: use the materialized payload table if present.
  const json = await opts.runner.getText(
    "SELECT json_object('replica', lower(hex(last_replica)), 'counter', CAST(last_counter AS TEXT)) \
     FROM tree_payload \
     WHERE node = ?1",
    [opts.nodeBytes],
  );
  if (json) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(json);
    } catch {
      // fall through to ops-table fallback
      parsed = null;
    }
    if (parsed && typeof parsed === 'object') {
      const replicaHex = (parsed as any).replica;
      const counterText = (parsed as any).counter;
      if (
        typeof replicaHex === 'string' &&
        typeof counterText === 'string' &&
        replicaHex.length > 0
      ) {
        let counter: bigint;
        try {
          counter = BigInt(counterText);
        } catch {
          counter = 0n;
        }

        if (counter > 0n) {
          try {
            const replica = hexToBytes(replicaHex);
            return deriveOpRefV0(opts.docId, { replica, counter });
          } catch {
            // fall through to ops-table fallback
          }
        }
      }
    }
  }

  // Fallback: query the op-log directly. This avoids relying on derived state (which can be stale
  // if materialization fails or is unavailable in a given environment).
  const opRefHex = await opts.runner.getText(
    "SELECT lower(hex(op_ref)) \
     FROM ops \
     WHERE node = ?1 AND op_ref IS NOT NULL AND (kind = 'payload' OR (kind = 'insert' AND payload IS NOT NULL)) \
     ORDER BY lamport DESC, replica DESC, counter DESC \
     LIMIT 1",
    [opts.nodeBytes],
  );
  if (!opRefHex || typeof opRefHex !== 'string') return null;
  try {
    const opRef = hexToBytes(opRefHex);
    return opRef.length === 16 ? opRef : null;
  } catch {
    return null;
  }
}

export function createTreecrdtSyncBackendFromClient(
  client: TreecrdtSyncBackendClient,
  docId: string,
  opts: {
    enablePendingSidecar?: boolean;
    maxLamport?: () => Promise<bigint> | bigint;
  } = {},
): SyncBackend<Operation> {
  const pending = opts.enablePendingSidecar
    ? (() => {
        if (!client.runner) throw new Error('enablePendingSidecar requires client.runner');
        return createTreecrdtSyncSqlitePendingOpsStore({ runner: client.runner, docId });
      })()
    : null;
  let pendingReady = false;
  const ensurePendingReady = async () => {
    if (!pending || pendingReady) return;
    await pending.init();
    pendingReady = true;
  };

  const defaultMaxLamport = async () => {
    const metaHeadLamport = client.meta?.headLamport;
    if (metaHeadLamport) return BigInt(await metaHeadLamport());
    if (client.ops.all) {
      const ops = await client.ops.all();
      const max = ops.reduce((acc, op) => Math.max(acc, op.meta.lamport), 0);
      return BigInt(max);
    }
    throw new Error('maxLamport: missing client.meta.headLamport and client.ops.all');
  };

  return {
    docId,

    maxLamport: async () => (opts.maxLamport ? await opts.maxLamport() : await defaultMaxLamport()),

    listOpRefs: async (filter: Filter) => {
      if ('all' in filter) {
        const refs = await client.opRefs.all();
        if (!pending) return refs;
        await ensurePendingReady();
        const pendingRefs = await pending.listPendingOpRefs();
        if (pendingRefs.length === 0) return refs;

        const byHex = new Map(refs.map((r) => [bytesToHex(r), r]));
        for (const r of pendingRefs) byHex.set(bytesToHex(r), r);
        return Array.from(byHex.values());
      }

      const parentHex = bytesToHex(filter.children.parent);
      const refs = await client.opRefs.children(parentHex);

      // Scoped sync often starts at a subtree root where the node's own payload opRef is not
      // discoverable via its parent (which may be outside scope). Include the node's latest
      // payload-writer opRef so `children(node)` can render the scope root label/value.
      if (!client.runner) return refs;
      const payloadWriter = await maybeLoadNodePayloadWriterOpRef({
        runner: client.runner,
        docId,
        nodeBytes: filter.children.parent,
      });
      if (!payloadWriter) return refs;

      const seen = new Set(refs.map((r) => bytesToHex(r)));
      const hex = bytesToHex(payloadWriter);
      if (seen.has(hex)) return refs;
      return [...refs, payloadWriter];
    },

    getOpsByOpRefs: async (opRefs) => client.ops.get(opRefs),

    applyOps: async (ops) => {
      if (ops.length === 0) return;
      await client.ops.appendMany(ops);
    },

    ...(pending
      ? {
          storePendingOps: async (ops) => {
            await ensurePendingReady();
            await pending.storePendingOps(ops);
          },
          listPendingOps: async () => {
            await ensurePendingReady();
            return pending.listPendingOps();
          },
          deletePendingOps: async (ops) => {
            await ensurePendingReady();
            await pending.deletePendingOps(ops);
          },
        }
      : {}),
  };
}
