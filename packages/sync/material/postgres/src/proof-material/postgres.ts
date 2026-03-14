import { Pool } from "pg";

import type { Operation } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";
import type {
  Capability,
  OpAuth,
  OpRef,
  SyncCapabilityMaterialStore,
  SyncOpAuthStore,
  SyncPendingOpsStore,
} from "@treecrdt/sync";
import { deriveOpRefV0 } from "@treecrdt/sync";
import { decodeTreecrdtSyncV0Operation, encodeTreecrdtSyncV0Operation } from "@treecrdt/sync/protobuf";

export type PostgresOpAuthStore = {
  init: () => Promise<void>;
  forDoc: (docId: string) => SyncOpAuthStore;
  close: () => Promise<void>;
};

export type PostgresCapabilityMaterialStore = {
  init: () => Promise<void>;
  forDoc: (docId: string) => SyncCapabilityMaterialStore;
  close: () => Promise<void>;
};

export type PostgresPendingOpsStore = {
  init: () => Promise<void>;
  forDoc: (docId: string) => SyncPendingOpsStore<Operation>;
  close: () => Promise<void>;
};

const OP_AUTH_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS treecrdt_sync_op_auth (
  doc_id TEXT NOT NULL,
  op_ref BYTEA NOT NULL,
  sig BYTEA NOT NULL,
  proof_ref BYTEA,
  created_at_ms BIGINT NOT NULL,
  PRIMARY KEY (doc_id, op_ref)
);
`;

const CAPABILITY_MATERIAL_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS treecrdt_sync_capability (
  doc_id TEXT NOT NULL,
  name TEXT NOT NULL,
  value TEXT NOT NULL,
  created_at_ms BIGINT NOT NULL,
  PRIMARY KEY (doc_id, name, value)
);
`;

const PENDING_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS treecrdt_sync_pending_ops (
  doc_id TEXT NOT NULL,
  op_ref BYTEA NOT NULL,
  op BYTEA NOT NULL,
  sig BYTEA NOT NULL,
  proof_ref BYTEA,
  reason TEXT NOT NULL,
  message TEXT,
  created_at_ms BIGINT NOT NULL,
  PRIMARY KEY (doc_id, op_ref)
);
`;

function toBytes(value: Uint8Array | Buffer): Uint8Array {
  return Uint8Array.from(value);
}

function dedupeLatestByKey<T>(values: T[], keyOf: (value: T) => string): T[] {
  const deduped = new Map<string, T>();
  for (const value of values) {
    deduped.set(keyOf(value), value);
  }
  return Array.from(deduped.values());
}

function insertOpAuthSql(entryCount: number): string {
  if (!Number.isInteger(entryCount) || entryCount <= 0) {
    throw new Error(`invalid op auth entry count: ${entryCount}`);
  }

  const rows: string[] = [];
  for (let i = 0; i < entryCount; i += 1) {
    const base = i * 5;
    rows.push(`($${base + 1}, $${base + 2}::bytea, $${base + 3}::bytea, $${base + 4}::bytea, $${base + 5})`);
  }

  return `
INSERT INTO treecrdt_sync_op_auth (doc_id, op_ref, sig, proof_ref, created_at_ms)
VALUES ${rows.join(",\n")}
ON CONFLICT (doc_id, op_ref)
DO UPDATE SET
  sig = EXCLUDED.sig,
  proof_ref = EXCLUDED.proof_ref,
  created_at_ms = EXCLUDED.created_at_ms
`;
}

function selectOpAuthByRefsSql(opRefCount: number): string {
  if (!Number.isInteger(opRefCount) || opRefCount <= 0) {
    throw new Error(`invalid opRefs length: ${opRefCount}`);
  }
  const placeholders = Array.from({ length: opRefCount }, (_value, index) => `$${index + 2}::bytea`).join(", ");
  return `
SELECT op_ref, sig, proof_ref
FROM treecrdt_sync_op_auth
WHERE doc_id = $1 AND op_ref IN (${placeholders})
`;
}

export function createOpAuthStore(opts: {
  postgresUrl: string;
  nowMs?: () => number;
}): PostgresOpAuthStore {
  const nowMs = opts.nowMs ?? (() => Date.now());
  const pool = new Pool({ connectionString: opts.postgresUrl });

  return {
    init: async () => {
      await pool.query(OP_AUTH_SCHEMA_SQL);
    },
    forDoc: (docId: string): SyncOpAuthStore => ({
      storeOpAuth: async (entries) => {
        if (entries.length === 0) return;
        const deduped = dedupeLatestByKey(entries, (entry) => bytesToHex(entry.opRef));

        const params: Array<string | number | Uint8Array | null> = [];
        for (const entry of deduped) {
          params.push(docId, entry.opRef, entry.auth.sig, entry.auth.proofRef ?? null, nowMs());
        }

        await pool.query(insertOpAuthSql(deduped.length), params);
      },

      getOpAuthByOpRefs: async (opRefs) => {
        if (opRefs.length === 0) return [];

        const res = await pool.query<{
          op_ref: Buffer;
          sig: Buffer;
          proof_ref: Buffer | null;
        }>(selectOpAuthByRefsSql(opRefs.length), [docId, ...opRefs]);

        const byOpRefHex = new Map<string, OpAuth>();
        for (const row of res.rows) {
          const opRef = toBytes(row.op_ref);
          const sig = toBytes(row.sig);
          const proofRef = row.proof_ref ? toBytes(row.proof_ref) : undefined;
          byOpRefHex.set(bytesToHex(opRef), { sig, ...(proofRef ? { proofRef } : {}) });
        }

        return opRefs.map((opRef) => byOpRefHex.get(bytesToHex(opRef)) ?? null);
      },
    }),
    close: async () => {
      await pool.end();
    },
  };
}

function insertCapabilitiesSql(capCount: number): string {
  if (!Number.isInteger(capCount) || capCount <= 0) {
    throw new Error(`invalid capability count: ${capCount}`);
  }

  const rows: string[] = [];
  for (let i = 0; i < capCount; i += 1) {
    const base = i * 4;
    rows.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4})`);
  }

  return `
INSERT INTO treecrdt_sync_capability (doc_id, name, value, created_at_ms)
VALUES ${rows.join(",\n")}
ON CONFLICT (doc_id, name, value)
DO UPDATE SET created_at_ms = EXCLUDED.created_at_ms
`;
}

function insertPendingOpsSql(entryCount: number): string {
  if (!Number.isInteger(entryCount) || entryCount <= 0) {
    throw new Error(`invalid pending op count: ${entryCount}`);
  }

  const rows: string[] = [];
  for (let i = 0; i < entryCount; i += 1) {
    const base = i * 8;
    rows.push(
      `($${base + 1}, $${base + 2}::bytea, $${base + 3}::bytea, $${base + 4}::bytea, $${base + 5}::bytea, $${base + 6}, $${base + 7}, $${base + 8})`
    );
  }

  return `
INSERT INTO treecrdt_sync_pending_ops (doc_id, op_ref, op, sig, proof_ref, reason, message, created_at_ms)
VALUES ${rows.join(",\n")}
ON CONFLICT (doc_id, op_ref)
DO UPDATE SET
  op = EXCLUDED.op,
  sig = EXCLUDED.sig,
  proof_ref = EXCLUDED.proof_ref,
  reason = EXCLUDED.reason,
  message = EXCLUDED.message,
  created_at_ms = EXCLUDED.created_at_ms
`;
}

export function createCapabilityMaterialStore(opts: {
  postgresUrl: string;
  nowMs?: () => number;
}): PostgresCapabilityMaterialStore {
  const nowMs = opts.nowMs ?? (() => Date.now());
  const pool = new Pool({ connectionString: opts.postgresUrl });

  return {
    init: async () => {
      await pool.query(CAPABILITY_MATERIAL_SCHEMA_SQL);
    },
    forDoc: (docId): SyncCapabilityMaterialStore => ({
      storeCapabilities: async (caps) => {
        if (caps.length === 0) return;
        const deduped = dedupeLatestByKey(caps, (cap) => `${cap.name}\u0000${cap.value}`);
        const params: Array<string | number> = [];
        for (const cap of deduped) {
          params.push(docId, cap.name, cap.value, nowMs());
        }
        await pool.query(insertCapabilitiesSql(deduped.length), params);
      },
      listCapabilities: async () => {
        const res = await pool.query<{ name: string; value: string }>(
          `
SELECT name, value
FROM treecrdt_sync_capability
WHERE doc_id = $1
ORDER BY created_at_ms ASC, name ASC, value ASC
`,
          [docId]
        );
        return res.rows.map((row) => ({ name: row.name, value: row.value } satisfies Capability));
      },
    }),
    close: async () => {
      await pool.end();
    },
  };
}

export function createPendingOpsStore(opts: {
  postgresUrl: string;
  nowMs?: () => number;
}): PostgresPendingOpsStore {
  const nowMs = opts.nowMs ?? (() => Date.now());
  const pool = new Pool({ connectionString: opts.postgresUrl });

  const opRefForOp = (docId: string, op: Operation): OpRef =>
    deriveOpRefV0(docId, {
      replica: op.meta.id.replica,
      counter: BigInt(op.meta.id.counter),
    });

  return {
    init: async () => {
      await pool.query(PENDING_SCHEMA_SQL);
    },
    forDoc: (docId: string): SyncPendingOpsStore<Operation> => ({
      init: async () => {
        await pool.query(PENDING_SCHEMA_SQL);
      },
      storePendingOps: async (entries) => {
        if (entries.length === 0) return;
        const deduped = dedupeLatestByKey(entries, (entry) => bytesToHex(opRefForOp(docId, entry.op)));

        const params: Array<string | number | Uint8Array | null> = [];
        for (const entry of deduped) {
          params.push(
            docId,
            opRefForOp(docId, entry.op),
            encodeTreecrdtSyncV0Operation(entry.op),
            entry.auth.sig,
            entry.auth.proofRef ?? null,
            entry.reason,
            entry.message ?? null,
            nowMs()
          );
        }

        await pool.query(insertPendingOpsSql(deduped.length), params);
      },
      listPendingOps: async () => {
        const res = await pool.query<{
          op: Buffer;
          sig: Buffer;
          proof_ref: Buffer | null;
          reason: string;
          message: string | null;
        }>(
          `
SELECT op, sig, proof_ref, reason, message
FROM treecrdt_sync_pending_ops
WHERE doc_id = $1
ORDER BY created_at_ms ASC, op_ref ASC
`,
          [docId]
        );

        return res.rows.map((row) => ({
          op: decodeTreecrdtSyncV0Operation(toBytes(row.op)),
          auth: {
            sig: toBytes(row.sig),
            ...(row.proof_ref ? { proofRef: toBytes(row.proof_ref) } : {}),
          },
          reason: row.reason === "missing_context" ? "missing_context" : (() => {
            throw new Error(`unexpected pending reason: ${row.reason}`);
          })(),
          ...(row.message ? { message: row.message } : {}),
        }));
      },
      listPendingOpRefs: async () => {
        const res = await pool.query<{ op_ref: Buffer }>(
          `
SELECT op_ref
FROM treecrdt_sync_pending_ops
WHERE doc_id = $1
ORDER BY created_at_ms ASC, op_ref ASC
`,
          [docId]
        );
        return res.rows.map((row) => toBytes(row.op_ref));
      },
      deletePendingOps: async (ops) => {
        if (ops.length === 0) return;
        const client = await pool.connect();
        try {
          await client.query("BEGIN");
          for (const op of ops) {
            await client.query(
              `
DELETE FROM treecrdt_sync_pending_ops
WHERE doc_id = $1 AND op_ref = $2::bytea
`,
              [docId, opRefForOp(docId, op)]
            );
          }
          await client.query("COMMIT");
        } catch (err) {
          await client.query("ROLLBACK");
          throw err;
        } finally {
          client.release();
        }
      },
    }),
    close: async () => {
      await pool.end();
    },
  };
}
