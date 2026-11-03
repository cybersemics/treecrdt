import type { Operation } from '@treecrdt/interface';
import { bytesToHex, replicaIdToBytes } from '@treecrdt/interface/ids';
import { sql, type Kysely } from '@treecrdt/interface/sqlite';
import {
  deriveOpRefV0,
  type Capability,
  type OpAuth,
  type OpRef,
  type PendingOp,
  type SyncCapabilityMaterialStore,
  type SyncOpAuthStore,
  type SyncPendingOpsStore,
} from '@treecrdt/sync-protocol';
import {
  decodeTreecrdtSyncV0Operation,
  encodeTreecrdtSyncV0Operation,
} from '@treecrdt/sync-protocol/protobuf';

import type { TreecrdtSyncSqliteDb } from '../sync-schema.js';

export type SqlitePendingOpsStore = SyncPendingOpsStore<Operation>;
export type SqliteOpAuthStore = SyncOpAuthStore & {
  init: () => Promise<void>;
};
export type SqliteCapabilityMaterialStore = SyncCapabilityMaterialStore & {
  init: () => Promise<void>;
};

const PENDING_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS treecrdt_sync_pending_ops (
  doc_id TEXT NOT NULL,
  op_ref BLOB NOT NULL,              -- 16 bytes
  op BLOB NOT NULL,                  -- protobuf bytes (sync/v0 Operation)
  sig BLOB NOT NULL,                 -- 64 bytes (Ed25519)
  proof_ref BLOB,                    -- 16 bytes (token id), nullable
  reason TEXT NOT NULL,              -- e.g. "missing_context"
  message TEXT,
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (doc_id, op_ref)
);
`;

const OP_AUTH_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS treecrdt_sync_op_auth (
  doc_id TEXT NOT NULL,
  op_ref BLOB NOT NULL,              -- 16 bytes
  sig BLOB NOT NULL,                 -- 64 bytes (Ed25519)
  proof_ref BLOB,                    -- 16 bytes (token id), nullable
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (doc_id, op_ref)
);
`;

const CAPABILITY_MATERIAL_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS treecrdt_sync_capability (
  doc_id TEXT NOT NULL,
  name TEXT NOT NULL,
  value TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (doc_id, name, value)
);
`;

function asBytes(value: Uint8Array | null | undefined): Uint8Array | null {
  if (value == null) return null;
  return value instanceof Uint8Array ? value : Uint8Array.from(value as any);
}

export function createPendingOpsStore(opts: {
  db: Kysely<TreecrdtSyncSqliteDb>;
  docId: string;
  nowMs?: () => number;
}): SqlitePendingOpsStore {
  const nowMs = opts.nowMs ?? (() => Date.now());
  const db = opts.db;

  const opRefForOp = (op: Operation): OpRef =>
    deriveOpRefV0(opts.docId, {
      replica: replicaIdToBytes(op.meta.id.replica),
      counter: BigInt(op.meta.id.counter),
    });

  return {
    init: async () => {
      await sql.raw(PENDING_SCHEMA_SQL).execute(db);
    },

    storePendingOps: async (pending) => {
      if (pending.length === 0) return;

      for (const p of pending) {
        const opRef = opRefForOp(p.op);
        const opBytes = encodeTreecrdtSyncV0Operation(p.op);
        const proofRef = p.auth.proofRef ?? null;
        const message = p.message ?? null;
        const createdAt = nowMs();
        await db
          .insertInto('treecrdt_sync_pending_ops')
          .values({
            doc_id: opts.docId,
            op_ref: opRef,
            op: opBytes,
            sig: p.auth.sig,
            proof_ref: proofRef,
            reason: p.reason,
            message,
            created_at_ms: createdAt,
          })
          .onConflict((oc) =>
            oc.columns(['doc_id', 'op_ref']).doUpdateSet({
              op: opBytes,
              sig: p.auth.sig,
              proof_ref: proofRef,
              reason: p.reason,
              message,
              created_at_ms: createdAt,
            }),
          )
          .execute();
      }
    },

    listPendingOps: async () => {
      const rows = await db
        .selectFrom('treecrdt_sync_pending_ops')
        .select(['op', 'sig', 'proof_ref', 'reason', 'message'])
        .where('doc_id', '=', opts.docId)
        .orderBy('created_at_ms', 'asc')
        .execute();

      return rows.map((r) => {
        const opBytes = asBytes(r.op)!;
        const op = decodeTreecrdtSyncV0Operation(opBytes);
        const sig = asBytes(r.sig)!;
        const proofRef = asBytes(r.proof_ref) ?? undefined;

        if (r.reason !== 'missing_context') {
          throw new Error(`unexpected pending reason: ${r.reason}`);
        }

        return {
          op,
          auth: { sig, ...(proofRef ? { proofRef } : {}) },
          reason: 'missing_context',
          ...(r.message ? { message: r.message } : {}),
        } satisfies PendingOp<Operation>;
      });
    },

    listPendingOpRefs: async () => {
      const rows = await db
        .selectFrom('treecrdt_sync_pending_ops')
        .select('op_ref')
        .where('doc_id', '=', opts.docId)
        .execute();
      return rows.map((r) => asBytes(r.op_ref)!);
    },

    deletePendingOps: async (ops) => {
      if (ops.length === 0) return;
      for (const op of ops) {
        await db
          .deleteFrom('treecrdt_sync_pending_ops')
          .where('doc_id', '=', opts.docId)
          .where('op_ref', '=', opRefForOp(op))
          .execute();
      }
    },
  };
}

export function createOpAuthStore(opts: {
  db: Kysely<TreecrdtSyncSqliteDb>;
  docId: string;
  nowMs?: () => number;
}): SqliteOpAuthStore {
  const nowMs = opts.nowMs ?? (() => Date.now());
  const db = opts.db;

  return {
    init: async () => {
      await sql.raw(OP_AUTH_SCHEMA_SQL).execute(db);
    },

    storeOpAuth: async (entries) => {
      if (entries.length === 0) return;

      for (const e of entries) {
        const proofRef = e.auth.proofRef ?? null;
        const createdAt = nowMs();
        await db
          .insertInto('treecrdt_sync_op_auth')
          .values({
            doc_id: opts.docId,
            op_ref: e.opRef,
            sig: e.auth.sig,
            proof_ref: proofRef,
            created_at_ms: createdAt,
          })
          .onConflict((oc) =>
            oc.columns(['doc_id', 'op_ref']).doUpdateSet({
              sig: e.auth.sig,
              proof_ref: proofRef,
              created_at_ms: createdAt,
            }),
          )
          .execute();
      }
    },

    getOpAuthByOpRefs: async (opRefs) => {
      if (opRefs.length === 0) return [];

      const rows = await db
        .selectFrom('treecrdt_sync_op_auth')
        .select(['op_ref', 'sig', 'proof_ref'])
        .where('doc_id', '=', opts.docId)
        .where('op_ref', 'in', opRefs)
        .execute();

      const byHex = new Map<string, OpAuth>();
      for (const r of rows) {
        const opRef = asBytes(r.op_ref)!;
        const sig = asBytes(r.sig)!;
        const proofRef = asBytes(r.proof_ref) ?? undefined;
        byHex.set(bytesToHex(opRef), { sig, ...(proofRef ? { proofRef } : {}) });
      }

      return opRefs.map((ref) => byHex.get(bytesToHex(ref)) ?? null);
    },
  };
}

export function createCapabilityMaterialStore(opts: {
  db: Kysely<TreecrdtSyncSqliteDb>;
  docId: string;
  nowMs?: () => number;
}): SqliteCapabilityMaterialStore {
  const nowMs = opts.nowMs ?? (() => Date.now());
  const db = opts.db;

  return {
    init: async () => {
      await sql.raw(CAPABILITY_MATERIAL_SCHEMA_SQL).execute(db);
    },

    storeCapabilities: async (caps) => {
      if (caps.length === 0) return;

      for (const cap of caps) {
        const createdAt = nowMs();
        await db
          .insertInto('treecrdt_sync_capability')
          .values({
            doc_id: opts.docId,
            name: cap.name,
            value: cap.value,
            created_at_ms: createdAt,
          })
          .onConflict((oc) =>
            oc.columns(['doc_id', 'name', 'value']).doUpdateSet({
              created_at_ms: createdAt,
            }),
          )
          .execute();
      }
    },

    listCapabilities: async () => {
      const rows = await db
        .selectFrom('treecrdt_sync_capability')
        .select(['name', 'value'])
        .where('doc_id', '=', opts.docId)
        .orderBy('name', 'asc')
        .orderBy('value', 'asc')
        .execute();
      return rows.map((row) => ({ name: row.name, value: row.value }) satisfies Capability);
    },
  };
}
