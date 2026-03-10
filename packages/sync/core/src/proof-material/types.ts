import type { Capability, OpAuth, OpRef, PendingOp } from "../types.js";

export interface SyncOpAuthStore {
  storeOpAuth: (entries: Array<{ opRef: OpRef; auth: OpAuth }>) => Promise<void>;
  getOpAuthByOpRefs: (opRefs: OpRef[]) => Promise<Array<OpAuth | null>>;
}

export interface SyncCapabilityMaterialStore {
  storeCapabilities: (caps: Capability[]) => Promise<void>;
  listCapabilities: () => Promise<Capability[]>;
}

export interface SyncPendingOpsStore<Op> {
  init: () => Promise<void>;
  storePendingOps: (ops: PendingOp<Op>[]) => Promise<void>;
  listPendingOps: () => Promise<PendingOp<Op>[]>;
  listPendingOpRefs: () => Promise<OpRef[]>;
  deletePendingOps: (ops: Op[]) => Promise<void>;
}

export type SyncAuthMaterialStore<Op> = {
  opAuth: SyncOpAuthStore;
  capabilities?: SyncCapabilityMaterialStore;
  pending?: SyncPendingOpsStore<Op>;
};

// "proof material" is the narrower term, but "auth material" is clearer once
// capability tokens and pending auth state are included in the same sidecar.
export type SyncProofMaterialStore<Op> = SyncAuthMaterialStore<Op>;
