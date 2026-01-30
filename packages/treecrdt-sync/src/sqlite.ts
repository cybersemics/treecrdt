import type { Operation } from "@treecrdt/interface";
import {
  ROOT_NODE_ID_HEX,
  TRASH_NODE_ID_HEX,
  bytesToHex,
  hexToBytes,
  nodeIdToBytes16,
  replicaIdToBytes,
} from "@treecrdt/interface/ids";
import type { SqliteRunner } from "@treecrdt/interface/sqlite";

import { deriveOpRefV0 } from "./opref.js";
import type { OpRef, PendingOp } from "./types.js";
import { decodeTreecrdtSyncV0Operation, encodeTreecrdtSyncV0Operation } from "./protobuf.js";
import type { TreecrdtScopeEvaluator } from "./treecrdt-auth.js";

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

export function createTreecrdtSqliteSubtreeScopeEvaluator(runner: SqliteRunner): TreecrdtScopeEvaluator {
  // Return a deterministic string so we can distinguish:
  // - missing row (no local context for that node) => "missing"
  // - NULL parent (chain end) => "null"
  // - otherwise => hex(parent)
  const parentSql = `
SELECT
  CASE
    WHEN t.node IS NULL THEN 'missing'
    WHEN t.parent IS NULL THEN 'null'
    ELSE lower(hex(t.parent))
  END AS parent_hex
FROM (SELECT 1) AS one
LEFT JOIN tree_nodes AS t ON t.node = ?1
`;

  const maxHops = 100_000;

  return async ({ node, scope }) => {
    const rootHex = bytesToHex(scope.root);
    const excludeHex = new Set((scope.exclude ?? []).map((b) => bytesToHex(b)));
    const maxDepth = scope.maxDepth;

    let curBytes = node;
    let curHex = bytesToHex(curBytes);
    let distance = 0;

    for (let hops = 0; hops < maxHops; hops += 1) {
      if (excludeHex.has(curHex)) return "deny";
      if (curHex === rootHex) {
        if (maxDepth !== undefined && distance > maxDepth) return "deny";
        return "allow";
      }

      // Treat the reserved ids as chain terminators even if they are not materialized.
      if (curHex === ROOT_NODE_ID_HEX || curHex === TRASH_NODE_ID_HEX) return "deny";

      // If we already traversed `maxDepth` edges without reaching `root`, the node cannot be within scope.
      if (maxDepth !== undefined && distance >= maxDepth) return "deny";

      const parentHex = await runner.getText(parentSql, [curBytes]);
      if (!parentHex) throw new Error("scope evaluator query returned empty result");

      if (parentHex === "missing") return "unknown";
      if (parentHex === "null") return "deny";

      // `tree_nodes.parent` is a NodeId (16 bytes).
      curBytes = nodeIdToBytes16(parentHex);
      curHex = parentHex;
      distance += 1;
    }

    // Defensive: cycles or extreme depth.
    return "unknown";
  };
}
