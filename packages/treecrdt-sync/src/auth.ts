import type { Capability, Hello, HelloAck, OpAuth } from "./types.js";

export type SyncOpPurpose = "reconcile" | "subscribe" | "reprocess_pending";

export type SyncAuthOpDisposition =
  | { status: "allow" }
  | { status: "pending_context"; message?: string };

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
   * Implementations MAY validate peer capabilities here and return capabilities
   * to include in `HelloAck.capabilities`.
   */
  onHello?: (hello: Hello, ctx: SyncAuthHelloContext) => Promise<Capability[]> | Capability[];

  /**
   * Initiator hook invoked when receiving `HelloAck`.
   */
  onHelloAck?: (ack: HelloAck, ctx: SyncAuthHelloContext) => Promise<void> | void;

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
    ctx: SyncAuthOpsContext
  ) => Promise<void | SyncAuthVerifyOpsResult> | void | SyncAuthVerifyOpsResult;

  /**
   * Optional hook to persist proofs (sidecar storage) after successful verification.
   */
  onVerifiedOps?: (ops: readonly Op[], auth: readonly OpAuth[], ctx: SyncAuthOpsContext) => Promise<void> | void;
}
