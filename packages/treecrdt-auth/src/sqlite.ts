import { ROOT_NODE_ID_HEX, TRASH_NODE_ID_HEX, bytesToHex, nodeIdToBytes16 } from "@treecrdt/interface/ids";
import type { SqliteRunner } from "@treecrdt/interface/sqlite";

import type { TreecrdtScopeEvaluator } from "./treecrdt-auth.js";

/**
 * Creates a subtree scope evaluator backed by the SQLite materialized tree state (`tree_nodes`).
 *
 * Used by the COSE+CWT auth layer to:
 * - enforce subtree-scoped capability tokens (is `node` within `scope.root`, honoring `maxDepth`/`exclude`?)
 * - validate delegated grants (the delegated scope must be within the proof scope)
 *
 * Semantics:
 * - returns `"allow"` if `node` is inside the subtree rooted at `scope.root`
 * - returns `"deny"` if `node` is definitely outside the subtree (or excluded)
 * - returns `"unknown"` if local context is insufficient (e.g. `tree_nodes` has no row for a needed ancestor)
 *
 * Known issues / tradeoffs:
 * - Performance: O(depth) per call with 1 SQL query per hop. For large sync batches this can be costly.
 * - Staleness: reads `tree_nodes` directly and does not force a rebuild/materialization pass; after reopen or
 *   if materialization is dirty, it may return `"unknown"` more often than necessary.
 * - Assumes `runner` is already scoped to the correct doc; `docId` is ignored.
 *
 * Potential improvements:
 * - Best-effort `treecrdt_ensure_materialized()` before traversal to reduce false `"unknown"`.
 * - Replace hop-by-hop queries with a single recursive CTE to walk ancestors.
 * - Add memoization / caching for repeated `(scope, node)` checks during a sync.
 */
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
