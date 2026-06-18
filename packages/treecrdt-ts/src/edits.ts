import type { Operation, ReplicaId } from './index.js';
import type { TreecrdtSqlitePlacement } from './sqlite.js';
import type {
  BoundTreecrdtEngineLocal,
  LocalWriteOptions,
  TreecrdtEngineLocal,
  TreecrdtEngineTree,
} from './engine.js';

export type DeleteAction = {
  type: 'delete';
  node: string;
};

export type MoveAction = {
  type: 'move';
  node: string;
  parent: string;
  placement: TreecrdtSqlitePlacement;
};

export type PayloadAction = {
  type: 'payload';
  node: string;
  payload: Uint8Array | null;
};

/**
 * A forward local write that reverses a previously captured local write.
 *
 * These are intentionally not oplog rewinds. Applying a plan mints new local ops, so undo/redo
 * remains mergeable with remote peers.
 */
export type Action = DeleteAction | MoveAction | PayloadAction;

export type Plan = {
  actions: Action[];
};

export type Target = {
  tree: TreecrdtEngineTree;
  local: TreecrdtEngineLocal;
};

export type Captured<T> = {
  result: T;
  operations: Operation[];
  undo: Plan;
};

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

function cloneBytes(bytes: Uint8Array | null): Uint8Array | null {
  return bytes === null ? null : new Uint8Array(bytes);
}

async function visiblePlacementBefore(
  tree: TreecrdtEngineTree,
  parent: string,
  node: string,
): Promise<TreecrdtSqlitePlacement> {
  const siblings = await tree.children(parent);
  const index = siblings.indexOf(node);
  if (index < 0) {
    throw new Error(`treecrdt: cannot capture undo position for non-visible node ${node}`);
  }
  if (index === 0) return { type: 'first' };
  return { type: 'after', after: siblings[index - 1]! };
}

async function currentMoveUndoAction(tree: TreecrdtEngineTree, node: string): Promise<Action> {
  const placement = await tree.placement?.(node);
  if (placement) {
    return {
      type: 'move',
      node,
      parent: placement.parent,
      placement: placement.placement,
    };
  }

  if (!(await tree.exists(node))) return { type: 'delete', node };

  const parent = await tree.parent(node);
  if (parent === null) {
    throw new Error(`treecrdt: cannot capture undo parent for root or missing node ${node}`);
  }
  return {
    type: 'move',
    node,
    parent,
    placement: await visiblePlacementBefore(tree, parent, node),
  };
}

/**
 * Capture the inverse of local writes performed through the supplied callback.
 *
 * The callback receives a bound local writer. Use that writer for every write that should be part
 * of the undo plan. The returned plan is ordered for direct application.
 */
export async function capture<T>(
  target: Target,
  replica: ReplicaId,
  fn: (local: BoundTreecrdtEngineLocal) => Promise<T>,
  defaults: LocalWriteOptions = {},
): Promise<Captured<T>> {
  const base = target.local.forReplica(replica, defaults);
  const operations: Operation[] = [];
  const actions: Action[] = [];

  const record = (op: Operation, action: Action) => {
    operations.push(op);
    actions.unshift(action);
  };

  const local: BoundTreecrdtEngineLocal = {
    insert: async (parent, node, placement, payload, opts) => {
      const op = await base.insert(parent, node, placement, payload, opts);
      record(op, { type: 'delete', node });
      return op;
    },
    move: async (node, newParent, placement, opts) => {
      const undo = await currentMoveUndoAction(target.tree, node);
      const op = await base.move(node, newParent, placement, opts);
      record(op, undo);
      return op;
    },
    delete: async (node, opts) => {
      const undo = await currentMoveUndoAction(target.tree, node);
      const op = await base.delete(node, opts);
      record(op, undo);
      return op;
    },
    payload: async (node, payload, opts) => {
      const previous = await target.tree.getPayload(node);
      const op = await base.payload(node, payload, opts);
      record(op, { type: 'payload', node, payload: cloneBytes(previous) });
      return op;
    },
  };

  const result = await fn(local);
  return { result, operations, undo: { actions } };
}

export async function applyToLocal(
  local: BoundTreecrdtEngineLocal,
  plan: Plan,
  opts?: LocalWriteOptions,
): Promise<Operation[]> {
  const operations: Operation[] = [];
  for (const action of plan.actions) {
    switch (action.type) {
      case 'delete':
        operations.push(await local.delete(action.node, opts));
        break;
      case 'move':
        operations.push(await local.move(action.node, action.parent, action.placement, opts));
        break;
      case 'payload':
        operations.push(await local.payload(action.node, cloneBytes(action.payload), opts));
        break;
    }
  }
  return operations;
}

export function apply(
  target: Target,
  replica: ReplicaId,
  plan: Plan,
  opts?: LocalWriteOptions,
): Promise<Operation[]> {
  return applyToLocal(target.local.forReplica(replica), plan, opts);
}

export async function undo(
  target: Target,
  replica: ReplicaId,
  edit: Undoable,
  opts?: LocalWriteOptions,
): Promise<UndoResult> {
  const applied = await capture(target, replica, (local) => applyToLocal(local, edit.undo, opts));
  return { operations: applied.operations, redo: applied.undo };
}

export async function redo(
  target: Target,
  replica: ReplicaId,
  edit: Redoable,
  opts?: LocalWriteOptions,
): Promise<RedoResult> {
  const applied = await capture(target, replica, (local) => applyToLocal(local, edit.redo, opts));
  return { operations: applied.operations, undo: applied.undo };
}

export const edits = {
  apply,
  applyToLocal,
  capture,
  redo,
  undo,
};
