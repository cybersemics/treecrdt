import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const __dirname = dirname(fileURLToPath(import.meta.url));

export type NativeOp = {
  lamport: bigint | number;
  replica: Uint8Array;
  counter: bigint | number;
  kind: string;
  parent?: Uint8Array | null;
  node: Uint8Array;
  newParent?: Uint8Array | null;
  orderKey?: Uint8Array | null;
  payload?: Uint8Array | null;
  knownState?: Uint8Array | null;
};

export type NativeMaterializationChange = {
  kind: string;
  node: Uint8Array;
  parentBefore?: Uint8Array | null;
  parentAfter?: Uint8Array | null;
  payload?: Uint8Array | null;
  source?: NativeMaterializationSource | null;
};

export type NativeMaterializationSource = {
  operation: {
    id: {
      replica: Uint8Array;
      counter: bigint | number;
    };
    lamport: bigint | number;
  };
};

export type NativeMaterializationOutcome = {
  headSeq: bigint | number;
  changes: NativeMaterializationChange[];
};

export type NativeLocalOpResult = {
  op: NativeOp;
  outcome: NativeMaterializationOutcome;
};

/**
 * Result of the `local*IfUnchanged` methods, which write the op only if `token` is its token (see
 * `createAuthorizedLocalWriter`): `outcome` if the op was written, otherwise its `token`.
 */
export type NativeLocalOpCommit = {
  op: NativeOp;
  outcome?: NativeMaterializationOutcome | null;
  token?: string | null;
};

export type NativeBackend = {
  maxLamport(): bigint;
  listOpRefsAll(): Uint8Array[];
  listOpRefsChildren(parent: Uint8Array): Uint8Array[];
  listOpRefsChildrenWithParentPayload(parent: Uint8Array): Uint8Array[];
  opsSince(lamport: bigint, root: Uint8Array | null): NativeOp[];
  getOpsByOpRefs(opRefs: Uint8Array[]): NativeOp[];
  treeChildren(parent: Uint8Array, index?: number | null, length?: number | null): Uint8Array[];
  treeDump(): {
    node: Uint8Array;
    parent: Uint8Array | null;
    orderKey: Uint8Array | null;
    tombstone: boolean;
  }[];
  treeNodeCount(): bigint;
  treeParent(node: Uint8Array): Uint8Array | null;
  treeExists(node: Uint8Array): boolean;
  treePayload(node: Uint8Array): Uint8Array | null;
  replicaMaxCounter(replica: Uint8Array): bigint;
  applyOps(ops: NativeOp[]): NativeMaterializationOutcome;
  ensureMaterialized(): NativeMaterializationOutcome;
  localInsert(
    replica: Uint8Array,
    parent: Uint8Array,
    node: Uint8Array,
    placement: string,
    after: Uint8Array | null,
    payload: Uint8Array | null,
  ): NativeLocalOpResult;
  localInsertIfUnchanged(
    replica: Uint8Array,
    parent: Uint8Array,
    node: Uint8Array,
    placement: string,
    after: Uint8Array | null,
    payload: Uint8Array | null,
    token: string | null,
  ): NativeLocalOpCommit;
  localMove(
    replica: Uint8Array,
    node: Uint8Array,
    newParent: Uint8Array,
    placement: string,
    after: Uint8Array | null,
  ): NativeLocalOpResult;
  localMoveIfUnchanged(
    replica: Uint8Array,
    node: Uint8Array,
    newParent: Uint8Array,
    placement: string,
    after: Uint8Array | null,
    token: string | null,
  ): NativeLocalOpCommit;
  localDelete(replica: Uint8Array, node: Uint8Array): NativeLocalOpResult;
  localDeleteIfUnchanged(
    replica: Uint8Array,
    node: Uint8Array,
    token: string | null,
  ): NativeLocalOpCommit;
  localPayload(
    replica: Uint8Array,
    node: Uint8Array,
    payload: Uint8Array | null,
  ): NativeLocalOpResult;
  localPayloadIfUnchanged(
    replica: Uint8Array,
    node: Uint8Array,
    payload: Uint8Array | null,
    token: string | null,
  ): NativeLocalOpCommit;
};

export type NativeFactory = {
  ensureSchema(): void;
  resetForTests(): void;
  resetDocForTests(docId: string): void;
  open(docId: string): NativeBackend;
};

type NativeExports = {
  PgFactory: new (url: string) => NativeFactory;
};

// NOTE: we vendor the built binary into `native/` during `pnpm run build:native`.
export function loadNative(): NativeExports {
  const modPath = join(__dirname, '..', 'native', 'treecrdt-postgres-napi.node');
  return require(modPath) as NativeExports;
}
