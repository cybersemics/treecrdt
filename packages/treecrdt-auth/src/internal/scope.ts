import type { Operation } from '@treecrdt/interface';
import type { LocalWriteAuthPreState } from '@treecrdt/interface/engine';
import { bytesToHex, nodeIdToBytes16, ROOT_NODE_ID_HEX } from '@treecrdt/interface/ids';

import { getField, toNumber } from './claims.js';

type TreecrdtSubtreeScope = {
  root: Uint8Array;
  maxDepth?: number;
  exclude?: Uint8Array[];
};

export type TreecrdtScopeEvaluation = 'allow' | 'deny' | 'unknown' | 'absent';

export type TreecrdtScopeEvaluatorInput = {
  docId: string;
  node: Uint8Array;
  scope: TreecrdtSubtreeScope;
};

export type TreecrdtScopeEvaluator = ((
  opts: TreecrdtScopeEvaluatorInput,
) => TreecrdtScopeEvaluation | Promise<TreecrdtScopeEvaluation>) & {
  /**
   * Evaluate `node` through an explicit immediate parent instead of consulting
   * the node's current materialized parent.
   *
   * Local SQLite authorization uses this after a structural write has been
   * prepared so the decision is still based on the pre-write ancestry. An
   * evaluator that cannot honor the override should omit this method; auth then
   * fails closed with `unknown`.
   */
  evaluateWithParent?: (
    opts: TreecrdtScopeEvaluatorInput & { parent: Uint8Array | null },
  ) => TreecrdtScopeEvaluation | Promise<TreecrdtScopeEvaluation>;
};

export type ScopeTri = 'allow' | 'deny' | 'unknown';

export function triOr(a: ScopeTri, b: ScopeTri): ScopeTri {
  if (a === 'allow' || b === 'allow') return 'allow';
  if (a === 'unknown' || b === 'unknown') return 'unknown';
  return 'deny';
}

export function triAnd(a: ScopeTri, b: ScopeTri): ScopeTri {
  if (a === 'deny' || b === 'deny') return 'deny';
  if (a === 'unknown' || b === 'unknown') return 'unknown';
  return 'allow';
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

type RequiredScopeCheck = {
  node: Uint8Array;
  actions: string[];
  allowAbsent?: boolean;
  knownAbsent?: boolean;
  parentBefore?: Uint8Array | null;
};

function checkedPreWriteState(
  op: Operation,
  preWriteState?: LocalWriteAuthPreState,
): LocalWriteAuthPreState | undefined {
  if (!preWriteState) return undefined;
  if (
    bytesToHex(nodeIdToBytes16(preWriteState.node)) !== bytesToHex(nodeIdToBytes16(op.kind.node))
  ) {
    throw new Error('pre-write auth state does not match operation node');
  }
  if (!preWriteState.existed && preWriteState.parent !== null) {
    throw new Error('absent pre-write auth state cannot have a parent');
  }
  return preWriteState;
}

function sourceScopeCheck(
  op: Operation,
  actions: string[],
  preWriteState?: LocalWriteAuthPreState,
  opts: { allowAbsent: boolean } = { allowAbsent: false },
): RequiredScopeCheck | null {
  const pre = checkedPreWriteState(op, preWriteState);
  const node = nodeIdToBytes16(op.kind.node);
  if (!pre) return { node, actions, allowAbsent: opts.allowAbsent };
  if (!pre.existed) {
    return opts.allowAbsent ? null : { node, actions, knownAbsent: true };
  }
  return {
    node,
    actions,
    parentBefore: pre.parent === null ? null : nodeIdToBytes16(pre.parent),
  };
}

function getRequiredScopeChecks(
  op: Operation,
  preWriteState?: LocalWriteAuthPreState,
): RequiredScopeCheck[] {
  const actions = requiredActionsForOp(op);
  switch (op.kind.type) {
    case 'insert': {
      const checks: RequiredScopeCheck[] = [{ node: nodeIdToBytes16(op.kind.parent), actions }];
      // Insert is an upsert/reparent. A new node has no source scope, but an
      // existing node must be authorized at both its old and new locations.
      const source = sourceScopeCheck(op, actions, preWriteState, { allowAbsent: true });
      if (source) checks.push(source);
      return checks;
    }
    case 'move': {
      // Require authorization for both the pre-write source and destination.
      const source = sourceScopeCheck(op, actions, preWriteState);
      return [...(source ? [source] : []), { node: nodeIdToBytes16(op.kind.newParent), actions }];
    }
    case 'delete':
    case 'tombstone':
    case 'payload': {
      const source = sourceScopeCheck(op, actions, preWriteState);
      return source ? [source] : [];
    }
    default: {
      const _exhaustive: never = op.kind;
      throw new Error(`unknown op kind: ${String((_exhaustive as any)?.type)}`);
    }
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
  allowAbsent?: boolean;
  knownAbsent?: boolean;
  parentBefore?: Uint8Array | null;
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

  if (opts.knownAbsent) return opts.allowAbsent ? 'allow' : 'unknown';

  if (!opts.scopeEvaluator) return 'unknown';
  const evaluation =
    opts.parentBefore !== undefined
      ? opts.scopeEvaluator.evaluateWithParent
        ? await opts.scopeEvaluator.evaluateWithParent({
            docId: opts.docId,
            node: opts.node,
            scope,
            parent: opts.parentBefore,
          })
        : 'unknown'
      : await opts.scopeEvaluator({ docId: opts.docId, node: opts.node, scope });
  if (evaluation === 'absent') return opts.allowAbsent ? 'allow' : 'unknown';
  return evaluation;
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

export async function capsAllowsOp(opts: {
  caps: unknown;
  docId: string;
  op: Operation;
  scopeEvaluator?: TreecrdtScopeEvaluator;
  preWriteState?: LocalWriteAuthPreState;
}): Promise<ScopeTri> {
  if (!Array.isArray(opts.caps)) throw new Error('capability token must be a v1 capability token');

  const checks = getRequiredScopeChecks(opts.op, opts.preWriteState);
  let overall: ScopeTri = 'allow';

  for (const check of checks) {
    let best: ScopeTri = 'deny';
    for (const cap of opts.caps) {
      best = triOr(
        best,
        await capAllowsNode({
          cap,
          docId: opts.docId,
          node: check.node,
          requiredActions: check.actions,
          scopeEvaluator: opts.scopeEvaluator,
          allowAbsent: check.allowAbsent,
          knownAbsent: check.knownAbsent,
          parentBefore: check.parentBefore,
        }),
      );
      if (best === 'allow') break;
    }
    overall = triAnd(overall, best);
  }

  return overall;
}
