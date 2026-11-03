import type { Operation } from '@treecrdt/interface';
import { bytesToHex } from '@treecrdt/interface/ids';
import { sql, type Kysely, type TreecrdtDb } from '@treecrdt/interface/sqlite';
import { deriveOpRefV0 } from '@treecrdt/sync-protocol';
import type { Filter, OpRef, SyncBackend } from '@treecrdt/sync-protocol';

import { createPendingOpsStore } from './proof-material/index.js';
import type { TreecrdtSyncSqliteDb } from './sync-schema.js';

export type TreecrdtSyncBackendClient = {
  db?: Kysely<TreecrdtDb>;
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
    appendMany: (ops: Operation[]) => Promise<unknown>;
  };
};

async function maybeLoadNodePayloadWriterOpRef(opts: {
  db: Kysely<TreecrdtDb>;
  docId: string;
  nodeBytes: Uint8Array;
}): Promise<Uint8Array | null> {
  // `tree_payload` is a derived table; ensure it is up-to-date before reading.
  try {
    await sql`select treecrdt_ensure_materialized()`.execute(opts.db);
  } catch {
    // Best-effort. Some backends may not expose this UDF (non-SQLite); callers will ignore null.
  }

  // Fast path: use the materialized payload table if present.
  const payloadRow = await opts.db
    .selectFrom('tree_payload')
    .select(['last_replica', 'last_counter'])
    .where('node', '=', opts.nodeBytes)
    .executeTakeFirst();

  if (payloadRow && payloadRow.last_replica && payloadRow.last_counter > 0) {
    try {
      const replica =
        payloadRow.last_replica instanceof Uint8Array
          ? payloadRow.last_replica
          : Uint8Array.from(payloadRow.last_replica as any);
      return deriveOpRefV0(opts.docId, {
        replica,
        counter: BigInt(payloadRow.last_counter),
      });
    } catch {
      // fall through to ops-table fallback
    }
  }

  // Fallback: query the op-log directly. This avoids relying on derived state (which can be stale
  // if materialization fails or is unavailable in a given environment).
  const opRow = await opts.db
    .selectFrom('ops')
    .select('op_ref')
    .where('node', '=', opts.nodeBytes)
    .where('op_ref', 'is not', null)
    .where((eb) =>
      eb.or([
        eb('kind', '=', 'payload'),
        eb.and([eb('kind', '=', 'insert'), eb('payload', 'is not', null)]),
      ]),
    )
    .orderBy('lamport', 'desc')
    .orderBy('replica', 'desc')
    .orderBy('counter', 'desc')
    .limit(1)
    .executeTakeFirst();

  const opRef = opRow?.op_ref;
  if (!opRef) return null;
  const bytes = opRef instanceof Uint8Array ? opRef : Uint8Array.from(opRef as any);
  return bytes.length === 16 ? bytes : null;
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
        if (!client.db) throw new Error('enablePendingSidecar requires client.db');
        return createPendingOpsStore({
          db: client.db as unknown as Kysely<TreecrdtSyncSqliteDb>,
          docId,
        });
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
      if (!client.db) return refs;
      const payloadWriter = await maybeLoadNodePayloadWriterOpRef({
        db: client.db,
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
