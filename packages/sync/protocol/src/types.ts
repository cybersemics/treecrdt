import type { RibltCodeword16 } from "@treecrdt/riblt-wasm";
import type { ErrorCode, RibltFailureReason } from "./gen/sync/v0/messages_pb.js";

export { ErrorCode, RibltFailureReason } from "./gen/sync/v0/messages_pb.js";

export type Bytes = Uint8Array;
export type OpRef = Bytes; // fixed-width (16 bytes in v0)

/**
 * Scoped subtree filter rooted at `parent`.
 *
 * `SyncBackend.listOpRefs({ children: { parent } })` is defined as the opRef set required to
 * render and synchronize that subtree, not merely ops whose current parent equals `parent`.
 * Implementations MUST include:
 * - ops that affect the canonical child set of `parent`
 * - boundary-crossing moves relevant to that child set
 * - payload visibility needed for nodes that move into scope
 * - the scope root node's own latest payload-writer opRef, if any
 */
export type ChildrenFilter = { children: { parent: Bytes } };

export type Filter =
  | { all: Record<string, never> }
  | ChildrenFilter;

export type FilterSpec = {
  id: string;
  filter: Filter;
};

export type Capability = { name: string; value: string };

export type Hello = {
  capabilities: Capability[];
  filters: FilterSpec[];
  maxLamport: bigint;
};

export type RejectedFilter = {
  id: string;
  reason: ErrorCode;
  message?: string;
};

export type HelloAck = {
  capabilities: Capability[];
  acceptedFilters: string[];
  rejectedFilters: RejectedFilter[];
  maxLamport: bigint;
};

export type RibltCodeword = RibltCodeword16;

export type RibltCodewords = {
  filterId: string;
  round: number;
  startIndex: bigint;
  codewords: RibltCodeword[];
};

export type RibltMore = {
  codewordsReceived: bigint;
  credits: number;
};

export type RibltDecoded = {
  senderMissing: OpRef[];
  receiverMissing: OpRef[];
  codewordsReceived: bigint;
};

export type RibltFailed = {
  reason: RibltFailureReason;
  message?: string;
};

export type RibltStatus = {
  filterId: string;
  round: number;
  payload:
    | { case: "more"; value: RibltMore }
    | { case: "decoded"; value: RibltDecoded }
    | { case: "failed"; value: RibltFailed };
};

export type OpAuth = {
  sig: Bytes;
  proofRef?: Bytes;
};

export type PendingOpReason = "missing_context";

export type PendingOp<Op> = {
  op: Op;
  auth: OpAuth;
  reason: PendingOpReason;
  message?: string;
};

export type OpsBatch<Op> = {
  filterId: string;
  ops: Op[];
  auth?: OpAuth[];
  done: boolean;
};

export type Subscribe = {
  subscriptionId: string;
  filter: Filter;
};

export type SubscribeAck = {
  subscriptionId: string;
  currentLamport: bigint;
};

export type Unsubscribe = {
  subscriptionId: string;
};

export type SyncError = {
  code: ErrorCode;
  message: string;
  filterId?: string;
  subscriptionId?: string;
};

export type SyncMessagePayload<Op> =
  | { case: "hello"; value: Hello }
  | { case: "helloAck"; value: HelloAck }
  | { case: "ribltCodewords"; value: RibltCodewords }
  | { case: "ribltStatus"; value: RibltStatus }
  | { case: "opsBatch"; value: OpsBatch<Op> }
  | { case: "subscribe"; value: Subscribe }
  | { case: "subscribeAck"; value: SubscribeAck }
  | { case: "unsubscribe"; value: Unsubscribe }
  | { case: "error"; value: SyncError };

export type SyncMessage<Op> = {
  v: 0;
  docId: string;
  payload: SyncMessagePayload<Op>;
};

export interface SyncBackend<Op> {
  docId: string;
  maxLamport(): Promise<bigint>;
  /**
   * Return the opRef set for a sync filter.
   *
   * For `children(parent)`, implementations MUST follow `ChildrenFilter` semantics so all
   * backends advertise the same subtree state during reconcile and subscribe flows.
   */
  listOpRefs(filter: Filter): Promise<OpRef[]>;
  getOpsByOpRefs(opRefs: OpRef[]): Promise<Op[]>;
  applyOps(ops: Op[]): Promise<void>;

  /**
   * Optional: persist ops that were structurally valid (signatures/capabilities)
   * but could not be authorized due to missing local context (fail-closed).
   *
   * Implementations SHOULD store these in the same SQLite database as the CRDT
   * state (sidecar tables), not as separate files.
   */
  storePendingOps?: (ops: PendingOp<Op>[]) => Promise<void>;

  /**
   * Optional: return ops previously stored via `storePendingOps`.
   *
   * These ops MUST NOT be applied to CRDT state until re-verified.
   */
  listPendingOps?: () => Promise<PendingOp<Op>[]>;

  /**
   * Optional: remove pending ops after they have been applied.
   *
   * Implementations SHOULD identify ops by op_id/op_ref and ignore unknown entries.
   */
  deletePendingOps?: (ops: Op[]) => Promise<void>;
}
