import type { Operation, ReplicaId } from './index.js';
import type { TreecrdtSqlitePlacement } from './sqlite.js';
import type {
  BoundTreecrdtEngineLocal,
  EngineHistory,
  LocalEditAction,
  LocalEditPlan,
  LocalMoveAction,
  LocalWriteOptions,
  OperationEdit,
  TreecrdtEngineLocal,
  TreecrdtEngineTree,
} from './engine.js';

/**
 * A forward local write that reverses a previously captured local write.
 *
 * These are intentionally not oplog rewinds. Applying a plan mints new local ops, so undo/redo
 * remains mergeable with remote peers.
 */
type Action = LocalEditAction;

export type Plan = LocalEditPlan;

type Target = {
  tree: TreecrdtEngineTree;
  local: TreecrdtEngineLocal;
};

type HistoryTarget = Target & {
  history: EngineHistory;
};

export type CapturedPlan<T> = {
  result: T;
  operations: Operation[];
  undo: Plan;
};

export type Captured<T> = {
  result: T;
  operations: Operation[];
};

export type { OperationEdit };

export type Undoable = {
  undo: Plan;
};

export type Redoable = {
  redo: Plan;
};

export type UndoResult = {
  operations: Operation[];
  redo: Plan;
};

export type RedoResult = {
  operations: Operation[];
  undo: Plan;
};

/**
 * A callback or plan failed after one or more local operations had already committed.
 *
 * Callers must publish `operations`. `undo` reverses the committed prefix, and `remaining`
 * contains the failed plan action plus any actions that were not attempted.
 */
export class PartialEditError extends Error {
  readonly cause: unknown;
  readonly operations: Operation[];
  readonly undo?: Plan;
  readonly remaining?: Plan;

  constructor(cause: unknown, details: { operations: Operation[]; undo?: Plan; remaining?: Plan }) {
    const message = cause instanceof Error ? cause.message : String(cause);
    super(`treecrdt: edit partially applied: ${message}`);
    this.name = 'PartialEditError';
    this.cause = cause;
    this.operations = [...details.operations];
    this.undo = details.undo;
    this.remaining = details.remaining;
  }
}

class PlanApplyError extends Error {
  readonly cause: unknown;
  readonly remaining: Plan;

  constructor(cause: unknown, remaining: Plan) {
    const message = cause instanceof Error ? cause.message : String(cause);
    super(message);
    this.name = 'PlanApplyError';
    this.cause = cause;
    this.remaining = remaining;
  }
}

function cloneBytes(bytes: Uint8Array | null): Uint8Array | null {
  return bytes === null ? null : new Uint8Array(bytes);
}

async function visiblePlacementBefore(
  tree: TreecrdtEngineTree,
  parent: string,
  node: string,
): Promise<{ index: number; placement: TreecrdtSqlitePlacement }> {
  const siblings = await tree.children(parent);
  const index = siblings.indexOf(node);
  if (index < 0) {
    throw new Error(`treecrdt: cannot capture undo position for non-visible node ${node}`);
  }
  return {
    index,
    placement: index === 0 ? { type: 'first' } : { type: 'after', after: siblings[index - 1]! },
  };
}

async function currentStructuralUndoAction(
  tree: TreecrdtEngineTree,
  node: string,
  detached: 'reject' | 'payload',
): Promise<Action> {
  if (!(await tree.exists(node))) return { type: 'delete', node };

  const parent = await tree.parent(node);
  if (parent === null) {
    if (detached === 'payload') {
      return { type: 'payload', node, payload: cloneBytes(await tree.getPayload(node)) };
    }
    throw new Error(`treecrdt: cannot capture undo for detached node ${node}`);
  }
  const position = await visiblePlacementBefore(tree, parent, node);
  return {
    type: 'move',
    node,
    parent,
    ...position,
  };
}

function reverseActionGroups(groups: Action[][]): Plan {
  const actions: Action[] = [];
  for (const action of [...groups].reverse().flat()) {
    const previous = actions[actions.length - 1];
    if (
      action.type === 'payload' &&
      previous?.type === 'payload' &&
      action.node === previous.node
    ) {
      actions[actions.length - 1] = action;
    } else {
      actions.push(action);
    }
  }
  return { actions };
}

/**
 * Capture the inverse plan eagerly while local writes are performed.
 *
 * This is useful when a caller wants undo metadata immediately, but it may read current
 * parent/sibling/payload state on the write path. App integrations that have `engine.history`
 * should prefer `capture(...)` and defer inversion until undo time.
 */
export async function capturePlan<T>(
  target: Target,
  replica: ReplicaId,
  fn: (local: BoundTreecrdtEngineLocal) => Promise<T>,
  defaults: LocalWriteOptions = {},
): Promise<CapturedPlan<T>> {
  const base = target.local.forReplica(replica, defaults);
  const operations: Operation[] = [];
  const actionGroups: Action[][] = [];

  const record = (op: Operation, actions: Action[]) => {
    operations.push(op);
    actionGroups.push(actions);
  };

  const local: BoundTreecrdtEngineLocal = {
    insert: async (parent, node, placement, payload, opts) => {
      const wasVisible = await target.tree.exists(node);
      const previousParent = wasVisible ? await target.tree.parent(node) : null;
      if (wasVisible && previousParent === null) {
        throw new Error(`treecrdt: cannot capture undo for insert over detached node ${node}`);
      }
      const previousPayload =
        payload !== null ? cloneBytes(await target.tree.getPayload(node)) : null;
      const undo: Action[] =
        previousParent === null
          ? [
              ...(payload !== null
                ? ([{ type: 'payload', node, payload: previousPayload }] satisfies Action[])
                : []),
              { type: 'delete', node },
            ]
          : [
              ...(payload !== null
                ? ([
                    {
                      type: 'payload',
                      node,
                      payload: previousPayload,
                    },
                  ] satisfies Action[])
                : []),
              {
                type: 'move',
                node,
                parent: previousParent,
                ...(await visiblePlacementBefore(target.tree, previousParent, node)),
              },
            ];
      const op = await base.insert(parent, node, placement, payload, opts);
      record(op, undo);
      return op;
    },
    move: async (node, newParent, placement, opts) => {
      const undo = await currentStructuralUndoAction(target.tree, node, 'reject');
      const op = await base.move(node, newParent, placement, opts);
      record(op, [undo]);
      return op;
    },
    delete: async (node, opts) => {
      const undo = await currentStructuralUndoAction(target.tree, node, 'payload');
      const op = await base.delete(node, opts);
      record(op, [undo]);
      return op;
    },
    payload: async (node, payload, opts) => {
      const wasVisible = await target.tree.exists(node);
      const previous = await target.tree.getPayload(node);
      const op = await base.payload(node, payload, opts);
      record(op, [
        { type: 'payload', node, payload: cloneBytes(previous) },
        ...(!wasVisible ? ([{ type: 'delete', node }] satisfies Action[]) : []),
      ]);
      return op;
    },
  };

  try {
    const result = await fn(local);
    return { result, operations, undo: reverseActionGroups(actionGroups) };
  } catch (error) {
    if (operations.length === 0) {
      throw error instanceof PlanApplyError ? error.cause : error;
    }
    throw new PartialEditError(error instanceof PlanApplyError ? error.cause : error, {
      operations,
      undo: reverseActionGroups(actionGroups),
      ...(error instanceof PlanApplyError ? { remaining: error.remaining } : {}),
    });
  }
}

/**
 * Capture the local operations produced by the callback.
 *
 * This avoids inverse reads on the write path. Use `undo(...)` with a history-capable engine to
 * derive the inverse later from the operation log.
 */
export async function capture<T>(
  target: Target,
  replica: ReplicaId,
  fn: (local: BoundTreecrdtEngineLocal) => Promise<T>,
  defaults: LocalWriteOptions = {},
): Promise<Captured<T>> {
  const base = target.local.forReplica(replica, defaults);
  const operations: Operation[] = [];

  const local: BoundTreecrdtEngineLocal = {
    insert: async (parent, node, placement, payload, opts) => {
      const op = await base.insert(parent, node, placement, payload, opts);
      operations.push(op);
      return op;
    },
    move: async (node, newParent, placement, opts) => {
      const op = await base.move(node, newParent, placement, opts);
      operations.push(op);
      return op;
    },
    delete: async (node, opts) => {
      const op = await base.delete(node, opts);
      operations.push(op);
      return op;
    },
    payload: async (node, payload, opts) => {
      const op = await base.payload(node, payload, opts);
      operations.push(op);
      return op;
    },
  };

  try {
    const result = await fn(local);
    return { result, operations };
  } catch (error) {
    if (operations.length === 0) throw error;
    throw new PartialEditError(error, { operations });
  }
}

async function resolveMovePlacement(tree: TreecrdtEngineTree, action: LocalMoveAction) {
  if (action.placement.type !== 'after') return action.placement;

  const siblings = (await tree.children(action.parent)).filter((node) => node !== action.node);
  if (siblings.includes(action.placement.after)) {
    return action.placement;
  }

  const index = Math.max(0, Math.min(action.index - 1, siblings.length));
  return index === 0
    ? ({ type: 'first' } as const)
    : ({ type: 'after', after: siblings[index - 1]! } as const);
}

async function applyPlan(
  target: Target,
  local: BoundTreecrdtEngineLocal,
  plan: Plan,
  opts?: LocalWriteOptions,
): Promise<void> {
  for (const [index, action] of plan.actions.entries()) {
    try {
      switch (action.type) {
        case 'delete':
          await local.delete(action.node, opts);
          break;
        case 'move':
          await local.move(
            action.node,
            action.parent,
            await resolveMovePlacement(target.tree, action),
            opts,
          );
          break;
        case 'payload':
          await local.payload(action.node, cloneBytes(action.payload), opts);
          break;
      }
    } catch (error) {
      throw new PlanApplyError(error, { actions: plan.actions.slice(index) });
    }
  }
}

/**
 * Revert a captured edit by minting new local operations.
 *
 * This is a force-revert: a newer undo operation can replace later remote writes to the same
 * node or payload. Applications that need selective undo should detect those conflicts first.
 * Structural edits whose prior state was a live, parentless node cannot be represented and reject.
 */
export async function undo(
  target: HistoryTarget,
  replica: ReplicaId,
  edit: OperationEdit,
  opts?: LocalWriteOptions,
): Promise<UndoResult>;
export async function undo(
  target: Target,
  replica: ReplicaId,
  edit: Undoable,
  opts?: LocalWriteOptions,
): Promise<UndoResult>;
export async function undo(
  target: Target | HistoryTarget,
  replica: ReplicaId,
  edit: OperationEdit | Undoable,
  opts?: LocalWriteOptions,
): Promise<UndoResult> {
  const undo =
    'undo' in edit ? edit.undo : await (target as Partial<HistoryTarget>).history?.invert(edit);
  if (!undo) {
    throw new Error('treecrdt: history inversion is not implemented by this engine');
  }
  const applied = await capturePlan(target, replica, (local) =>
    applyPlan(target, local, undo, opts),
  );
  return { operations: applied.operations, redo: applied.undo };
}

export async function redo(
  target: Target,
  replica: ReplicaId,
  edit: Redoable,
  opts?: LocalWriteOptions,
): Promise<RedoResult> {
  const applied = await capturePlan(target, replica, (local) =>
    applyPlan(target, local, edit.redo, opts),
  );
  return { operations: applied.operations, undo: applied.undo };
}

export const edits = {
  capture,
  capturePlan,
  redo,
  undo,
};
