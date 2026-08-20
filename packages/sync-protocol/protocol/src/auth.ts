import type { Capability, Hello, HelloAck, OpAuth } from './types.js';
import type { Filter } from './types.js';

export type SyncOpPurpose = 'local_write' | 'reconcile' | 'subscribe' | 'reprocess_pending';

export type SyncAuthOpDisposition =
  | { status: 'allow' }
  | { status: 'pending_context'; message?: string };

export type SyncAuthVerifyOpsResult = {
  dispositions: SyncAuthOpDisposition[];
};

export type SyncAuthOpsContext = {
  docId: string;
  purpose: SyncOpPurpose;
  filterId: string;
};

export type SyncAuthHelloContext = {
  docId: string;
};

export type SyncAuthAuthorizeFilterContext = {
  docId: string;
  purpose: 'hello' | 'subscribe';
  /** Validated peer capabilities with protocol-only direct-send negotiation removed. */
  capabilities: Capability[];
};

export type SyncAuthFilterOutgoingOpsContext = {
  docId: string;
  purpose: 'hello' | 'subscribe' | 'reconcile';
  filter: Filter;
  /** Validated peer capabilities with protocol-only direct-send negotiation removed. */
  capabilities: Capability[];
};

export interface SyncAuth<Op> {
  /**
   * Capabilities to advertise in `Hello.capabilities`.
   *
   * This is typically where an initiator provides authorization proofs
   * (e.g. COSE/CWT tokens) needed for read/write enforcement.
   */
  helloCapabilities?: (ctx: SyncAuthHelloContext) => Promise<Capability[]> | Capability[];

  /**
   * Responder hook invoked when receiving `Hello`.
   *
   * `Hello.capabilities` is a complete replacement snapshot. Implementations MAY
   * validate it here and return capabilities to include in `HelloAck.capabilities`.
   * Throwing rejects the snapshot before outgoing authorization can observe it.
   */
  onHello?: (hello: Hello, ctx: SyncAuthHelloContext) => Promise<Capability[]> | Capability[];

  /**
   * Initiator hook invoked when receiving `HelloAck`.
   *
   * `HelloAck.capabilities` is also a complete replacement snapshot. Throwing
   * rejects it before outgoing authorization can observe it.
   */
  onHelloAck?: (ack: HelloAck, ctx: SyncAuthHelloContext) => Promise<void> | void;

  /**
   * Optional hook to authorize a requested read filter using peer-advertised capabilities.
   *
   * Implementations SHOULD throw to deny the filter.
   */
  authorizeFilter?: (filter: Filter, ctx: SyncAuthAuthorizeFilterContext) => Promise<void> | void;

  /**
   * Optional hook to hide/restrict outgoing ops based on peer-advertised capabilities.
   *
   * Returns a boolean allow-list aligned with `ops`.
   */
  filterOutgoingOps?: (
    ops: readonly Op[],
    ctx: SyncAuthFilterOutgoingOpsContext,
  ) => Promise<boolean[]> | boolean[];

  /**
   * Produce auth metadata aligned with a batch of outbound ops.
   */
  signOps?: (ops: readonly Op[], ctx: SyncAuthOpsContext) => Promise<OpAuth[]> | OpAuth[];

  /**
   * Verify an inbound batch. Throw to reject the batch.
   *
   * `auth` may be `undefined` if the sender did not attach auth metadata.
   */
  verifyOps?: (
    ops: readonly Op[],
    auth: readonly OpAuth[] | undefined,
    ctx: SyncAuthOpsContext,
  ) => Promise<void | SyncAuthVerifyOpsResult> | void | SyncAuthVerifyOpsResult;

  /**
   * Optional hook to persist proofs (sidecar storage) after successful verification.
   */
  onVerifiedOps?: (
    ops: readonly Op[],
    auth: readonly OpAuth[],
    ctx: SyncAuthOpsContext,
  ) => Promise<void> | void;
}
