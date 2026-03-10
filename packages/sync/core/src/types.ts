import type { RibltCodeword16 } from "@treecrdt/riblt-wasm";
import type { ErrorCode, RibltFailureReason } from "./gen/sync/v0/messages_pb.js";

export { ErrorCode, RibltFailureReason } from "./gen/sync/v0/messages_pb.js";

export type Bytes = Uint8Array;
export type OpRef = Bytes; // fixed-width (16 bytes in v0)

export type Filter =
  | { all: Record<string, never> }
  | { children: { parent: Bytes } };

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
