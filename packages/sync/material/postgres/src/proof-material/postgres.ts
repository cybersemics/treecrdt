import { Pool } from "pg";

import { bytesToHex } from "@treecrdt/interface/ids";
import type {
  Capability,
  OpAuth,
  OpRef,
  SyncCapabilityMaterialStore,
  SyncOpAuthStore,
} from "@treecrdt/sync";

export type PostgresSyncOpAuthStore = {
  init: () => Promise<void>;
  forDoc: (docId: string) => SyncOpAuthStore;
  close: () => Promise<void>;
};

export type PostgresSyncCapabilityMaterialStore = {
  init: () => Promise<void>;
  forDoc: (docId: string) => SyncCapabilityMaterialStore;
  close: () => Promise<void>;
};

// Backwards-compatible alias while the package API is in transition.
export type PostgresSyncCapabilityStore = PostgresSyncCapabilityMaterialStore;

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

export function createPostgresSyncOpAuthStore(opts: {
  postgresUrl: string;
  nowMs?: () => number;
}): PostgresSyncOpAuthStore {
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

export function createPostgresSyncCapabilityMaterialStore(opts: {
  postgresUrl: string;
  nowMs?: () => number;
}): PostgresSyncCapabilityMaterialStore {
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

export const createPostgresSyncCapabilityStore = createPostgresSyncCapabilityMaterialStore;
