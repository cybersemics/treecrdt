import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

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

export type NativeBackend = {
  maxLamport(): bigint;
  listOpRefsAll(): Uint8Array[];
  listOpRefsChildren(parent: Uint8Array): Uint8Array[];
  opsSince(lamport: bigint, root: Uint8Array | null): NativeOp[];
  getOpsByOpRefs(opRefs: Uint8Array[]): NativeOp[];
  treeChildren(parent: Uint8Array): Uint8Array[];
  treeChildrenPage(
    parent: Uint8Array,
    cursorOrderKey: Uint8Array | null,
    cursorNode: Uint8Array | null,
    limit: number
  ): { node: Uint8Array; orderKey: Uint8Array | null }[];
  treeDump(): { node: Uint8Array; parent: Uint8Array | null; orderKey: Uint8Array | null; tombstone: boolean }[];
  treeNodeCount(): bigint;
  replicaMaxCounter(replica: Uint8Array): bigint;
  applyOps(ops: NativeOp[]): void;
  localInsert(
    replica: Uint8Array,
    parent: Uint8Array,
    node: Uint8Array,
    placement: string,
    after: Uint8Array | null,
    payload: Uint8Array | null
  ): NativeOp;
  localMove(
    replica: Uint8Array,
    node: Uint8Array,
    newParent: Uint8Array,
    placement: string,
    after: Uint8Array | null
  ): NativeOp;
  localDelete(replica: Uint8Array, node: Uint8Array): NativeOp;
  localPayload(replica: Uint8Array, node: Uint8Array, payload: Uint8Array | null): NativeOp;
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
  const modPath = join(__dirname, "..", "native", "treecrdt-postgres-napi.node");
  return require(modPath) as NativeExports;
}
