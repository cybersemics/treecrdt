import type { Operation } from '@treecrdt/interface';
import { bytesToHex, ROOT_NODE_ID_HEX } from '@treecrdt/interface/ids';

import { getField, toNumber } from './claims.js';

type TreecrdtSubtreeScope = {
  root: Uint8Array;
  maxDepth?: number;
  exclude?: Uint8Array[];
};

export type TreecrdtScopeEvaluator = (opts: {
  docId: string;
  node: Uint8Array;
  scope: TreecrdtSubtreeScope;
}) => 'allow' | 'deny' | 'unknown' | Promise<'allow' | 'deny' | 'unknown'>;

export type ScopeTri = 'allow' | 'deny' | 'unknown';

export function triOr(a: ScopeTri, b: ScopeTri): ScopeTri {
  if (a === 'allow' || b === 'allow') return 'allow';
  if (a === 'unknown' || b === 'unknown') return 'unknown';
  return 'deny';
}

export function expandCapabilityActions(actions: readonly unknown[]): Set<string> {
  const actionSet = new Set(actions.map(String));
  // Keep in sync with capAllowsNode convenience rules.
  if (
    actionSet.has('write_structure') ||
    actionSet.has('write_payload') ||
    actionSet.has('delete') ||
    actionSet.has('tombstone')
  ) {
    actionSet.add('read_structure');
  }
  if (actionSet.has('write_payload')) actionSet.add('read_payload');
  return actionSet;
}

function requiredActionsForOp(op: Operation): string[] {
  switch (op.kind.type) {
    case 'insert':
      return op.kind.payload ? ['write_structure', 'write_payload'] : ['write_structure'];
    case 'move':
      return ['write_structure'];
    case 'delete':
      return ['delete'];
    case 'tombstone':
      return ['tombstone'];
    case 'payload':
      return ['write_payload'];
  }
}

export function parseScope(res: unknown): TreecrdtSubtreeScope {
  if (!res || typeof res !== 'object') throw new Error('capability res missing or not an object');

  const root = getField(res, 'root') as unknown;
  if (!(root instanceof Uint8Array)) throw new Error('capability res.root missing or not bytes');

  const maxDepthRaw = getField(res, 'max_depth');
  const maxDepth = toNumber(maxDepthRaw, 'max_depth');

  const excludeRaw = getField(res, 'exclude');
  let exclude: Uint8Array[] | undefined;
  if (excludeRaw !== undefined && excludeRaw !== null) {
    if (!Array.isArray(excludeRaw)) throw new Error('capability res.exclude must be an array');
    exclude = excludeRaw.map((v) => {
      if (!(v instanceof Uint8Array))
        throw new Error('capability res.exclude entries must be bytes');
      return v;
    });
  }

  return { root, ...(maxDepth !== undefined ? { maxDepth } : {}), ...(exclude ? { exclude } : {}) };
}

export function isDocWideScope(scope: TreecrdtSubtreeScope): boolean {
  return (
    bytesToHex(scope.root) === ROOT_NODE_ID_HEX &&
    scope.maxDepth === undefined &&
    (!scope.exclude || scope.exclude.length === 0)
  );
}

export async function capAllowsNode(opts: {
  cap: unknown;
  docId: string;
  node: Uint8Array;
  requiredActions: readonly string[];
  scopeEvaluator?: TreecrdtScopeEvaluator;
}): Promise<ScopeTri> {
  const res = getField(opts.cap, 'res');
  const actions = getField(opts.cap, 'actions');
  if (!res || typeof res !== 'object') return 'deny';
  if (!Array.isArray(actions)) return 'deny';
  if (getField(res, 'doc_id') !== opts.docId) return 'deny';

  const actionSet = expandCapabilityActions(actions);
  if (!opts.requiredActions.every((a) => actionSet.has(a))) return 'deny';

  const scope = parseScope(res);

  const rootHex = bytesToHex(scope.root);
  const nodeHex = bytesToHex(opts.node);

  // Common fast paths that do not require a tree view:
  if (nodeHex === rootHex) return 'allow';
  if (isDocWideScope(scope)) return 'allow';

  if (!opts.scopeEvaluator) return 'unknown';
  return await opts.scopeEvaluator({ docId: opts.docId, node: opts.node, scope });
}

export async function capsAllowsNodeAccess(opts: {
  caps: unknown;
  docId: string;
  node: Uint8Array;
  requiredActions: readonly string[];
  scopeEvaluator?: TreecrdtScopeEvaluator;
}): Promise<ScopeTri> {
  if (!Array.isArray(opts.caps)) throw new Error('capability token must be a v1 capability token');

  let best: ScopeTri = 'deny';
  for (const cap of opts.caps) {
    best = triOr(
      best,
      await capAllowsNode({
        cap,
        docId: opts.docId,
        node: opts.node,
        requiredActions: opts.requiredActions,
        scopeEvaluator: opts.scopeEvaluator,
      }),
    );
    if (best === 'allow') break;
  }

  return best;
}

export function capsAllowsOp(opts: { caps: unknown; docId: string; op: Operation }): boolean {
  if (!Array.isArray(opts.caps)) throw new Error('capability token must be a v1 capability token');

  // Ancestry is mutable CRDT state. Authorizing an operation against the receiver's current tree
  // makes the decision delivery-order dependent: a late, earlier move can change where the node
  // was at the operation's canonical position after the operation has already been accepted.
  // Until operations carry an authenticated causal ancestry witness, only doc-wide write grants
  // are state-independent and portable across peers. Scoped reads remain ancestry-aware through
  // capAllowsNode/capsAllowsNodeAccess.
  const requiredActions = requiredActionsForOp(opts.op);
  for (const cap of opts.caps) {
    const res = getField(cap, 'res');
    const actions = getField(cap, 'actions');
    if (!res || typeof res !== 'object' || !Array.isArray(actions)) continue;
    if (getField(res, 'doc_id') !== opts.docId) continue;
    const actionSet = expandCapabilityActions(actions);
    if (!requiredActions.every((action) => actionSet.has(action))) continue;
    if (isDocWideScope(parseScope(res))) return true;
  }

  return false;
}
