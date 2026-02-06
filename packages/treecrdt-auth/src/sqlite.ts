import { ROOT_NODE_ID_HEX, TRASH_NODE_ID_HEX, bytesToHex, nodeIdToBytes16 } from "@treecrdt/interface/ids";
import type { SqliteRunner } from "@treecrdt/interface/sqlite";

import type { TreecrdtScopeEvaluator } from "./treecrdt-auth.js";

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

