import type { Operation } from "@treecrdt/interface";
import { bytesToHex, hexToBytes, replicaIdToBytes } from "@treecrdt/interface/ids";
import type { SqliteRunner } from "@treecrdt/interface/sqlite";

import { deriveOpRefV0 } from "./opref.js";
import { decodeTreecrdtSyncV0Operation, encodeTreecrdtSyncV0Operation } from "./protobuf.js";
import type { OpAuth, OpRef, PendingOp } from "./types.js";

function hexToBytesStrict(hex: string, expectedLen: number, field: string): Uint8Array {
  const clean = hex.trim();
  if (clean.length !== expectedLen * 2) {
    throw new Error(
      `${field}: expected ${expectedLen} bytes (${expectedLen * 2} hex chars), got ${clean.length}: ${hex}`
    );
  }
  const bytes = hexToBytes(clean);
  if (bytes.length !== expectedLen) throw new Error(`${field}: expected ${expectedLen} bytes, got ${bytes.length}`);
  return bytes;
}

export type TreecrdtSyncSqlitePendingOpsStore = {
  init: () => Promise<void>;
  storePendingOps: (ops: PendingOp<Operation>[]) => Promise<void>;
  listPendingOps: () => Promise<PendingOp<Operation>[]>;
  listPendingOpRefs: () => Promise<OpRef[]>;
  deletePendingOps: (ops: Operation[]) => Promise<void>;
};

export type TreecrdtSyncSqliteOpAuthStore = {
  init: () => Promise<void>;
  storeOpAuth: (entries: Array<{ opRef: OpRef; auth: OpAuth }>) => Promise<void>;
  getOpAuthByOpRefs: (opRefs: OpRef[]) => Promise<Array<OpAuth | null>>;
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

export function createTreecrdtSyncSqlitePendingOpsStore(opts: {
  runner: SqliteRunner;
  docId: string;
  nowMs?: () => number;
}): TreecrdtSyncSqlitePendingOpsStore {
  const nowMs = opts.nowMs ?? (() => Date.now());

  const insertSql = `
INSERT OR REPLACE INTO treecrdt_sync_pending_ops
  (doc_id, op_ref, op, sig, proof_ref, reason, message, created_at_ms)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
RETURNING 1
`;

  const listSql = `
SELECT COALESCE(json_group_array(json(obj)), '[]') AS json
FROM (
  SELECT json_object(
    'op_hex', hex(op),
    'sig_hex', hex(sig),
    'proof_ref_hex', CASE WHEN proof_ref IS NULL THEN NULL ELSE hex(proof_ref) END,
    'reason', reason,
    'message', message
  ) AS obj
  FROM treecrdt_sync_pending_ops
  WHERE doc_id = ?1
  ORDER BY created_at_ms ASC
)
`;

  const listRefsSql = `
SELECT COALESCE(json_group_array(hex(op_ref)), '[]') AS json
FROM treecrdt_sync_pending_ops
WHERE doc_id = ?1
`;

  const deleteSql = `
DELETE FROM treecrdt_sync_pending_ops
WHERE doc_id = ?1 AND op_ref = ?2
RETURNING 1
`;

  const opRefForOp = (op: Operation): OpRef =>
    deriveOpRefV0(opts.docId, {
      replica: replicaIdToBytes(op.meta.id.replica),
      counter: BigInt(op.meta.id.counter),
    });

  return {
    init: async () => {
      await opts.runner.exec(PENDING_SCHEMA_SQL);
    },

    storePendingOps: async (pending) => {
      if (pending.length === 0) return;

      await opts.runner.exec("BEGIN");
      try {
        for (const p of pending) {
          const opRef = opRefForOp(p.op);
          const opBytes = encodeTreecrdtSyncV0Operation(p.op);
          const proofRef = p.auth.proofRef ?? null;
          const message = p.message ?? null;
          await opts.runner.getText(insertSql, [
            opts.docId,
            opRef,
            opBytes,
            p.auth.sig,
            proofRef,
            p.reason,
            message,
            nowMs(),
          ]);
        }
        await opts.runner.exec("COMMIT");
      } catch (err) {
        await opts.runner.exec("ROLLBACK");
        throw err;
      }
    },

    listPendingOps: async () => {
      const text = await opts.runner.getText(listSql, [opts.docId]);
      if (!text) return [];
      const rows = JSON.parse(text) as Array<{
        op_hex: string;
        sig_hex: string;
        proof_ref_hex: string | null;
        reason: string;
        message: string | null;
      }>;

      return rows.map((r) => {
        const opBytes = hexToBytes(r.op_hex);
        const op = decodeTreecrdtSyncV0Operation(opBytes);

        const sig = hexToBytesStrict(r.sig_hex, 64, "pending sig");
        const proofRef = r.proof_ref_hex ? hexToBytesStrict(r.proof_ref_hex, 16, "pending proof_ref") : undefined;

        if (r.reason !== "missing_context") {
          throw new Error(`unexpected pending reason: ${r.reason}`);
        }

        return {
          op,
          auth: { sig, ...(proofRef ? { proofRef } : {}) },
          reason: "missing_context",
          ...(r.message ? { message: r.message } : {}),
        } satisfies PendingOp<Operation>;
      });
    },

    listPendingOpRefs: async () => {
      const text = await opts.runner.getText(listRefsSql, [opts.docId]);
      if (!text) return [];
      const hexes = JSON.parse(text) as string[];
      return hexes.map((h) => hexToBytesStrict(h, 16, "pending op_ref"));
    },

    deletePendingOps: async (ops) => {
      if (ops.length === 0) return;
      await opts.runner.exec("BEGIN");
      try {
        for (const op of ops) {
          await opts.runner.getText(deleteSql, [opts.docId, opRefForOp(op)]);
        }
        await opts.runner.exec("COMMIT");
      } catch (err) {
        await opts.runner.exec("ROLLBACK");
        throw err;
      }
    },
  };
}

export function createTreecrdtSyncSqliteOpAuthStore(opts: {
  runner: SqliteRunner;
  docId: string;
  nowMs?: () => number;
}): TreecrdtSyncSqliteOpAuthStore {
  const nowMs = opts.nowMs ?? (() => Date.now());

  const insertSql = `
INSERT OR REPLACE INTO treecrdt_sync_op_auth
  (doc_id, op_ref, sig, proof_ref, created_at_ms)
VALUES (?1, ?2, ?3, ?4, ?5)
RETURNING 1
`;

  const selectByRefsSql = (n: number) => {
    if (!Number.isInteger(n) || n < 0) throw new Error(`invalid opRefs length: ${n}`);
    if (n === 0) throw new Error("selectByRefsSql requires at least 1 opRef");
    const placeholders = Array.from({ length: n }, (_v, i) => `?${i + 2}`).join(", ");
    return `
SELECT COALESCE(json_group_array(json(obj)), '[]') AS json
FROM (
  SELECT json_object(
    'op_ref_hex', hex(op_ref),
    'sig_hex', hex(sig),
    'proof_ref_hex', CASE WHEN proof_ref IS NULL THEN NULL ELSE hex(proof_ref) END
  ) AS obj
  FROM treecrdt_sync_op_auth
  WHERE doc_id = ?1 AND op_ref IN (${placeholders})
)
`;
  };

  return {
    init: async () => {
      await opts.runner.exec(OP_AUTH_SCHEMA_SQL);
    },

    storeOpAuth: async (entries) => {
      if (entries.length === 0) return;

      await opts.runner.exec("BEGIN");
      try {
        for (const e of entries) {
          const proofRef = e.auth.proofRef ?? null;
          await opts.runner.getText(insertSql, [opts.docId, e.opRef, e.auth.sig, proofRef, nowMs()]);
        }
        await opts.runner.exec("COMMIT");
      } catch (err) {
        await opts.runner.exec("ROLLBACK");
        throw err;
      }
    },

    getOpAuthByOpRefs: async (opRefs) => {
      if (opRefs.length === 0) return [];

      const sql = selectByRefsSql(opRefs.length);
      const text = await opts.runner.getText(sql, [opts.docId, ...opRefs]);
      if (!text) return opRefs.map(() => null);
      const rows = JSON.parse(text) as Array<{ op_ref_hex: string; sig_hex: string; proof_ref_hex: string | null }>;

      const byHex = new Map<string, OpAuth>();
      for (const r of rows) {
        const opRefHex = String(r.op_ref_hex).toLowerCase();
        const sig = hexToBytesStrict(r.sig_hex, 64, "op_auth sig");
        const proofRef = r.proof_ref_hex ? hexToBytesStrict(r.proof_ref_hex, 16, "op_auth proof_ref") : undefined;
        byHex.set(opRefHex, { sig, ...(proofRef ? { proofRef } : {}) });
      }

      return opRefs.map((ref) => byHex.get(bytesToHex(ref)) ?? null);
    },
  };
}
