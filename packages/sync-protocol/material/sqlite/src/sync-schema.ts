import type { TreecrdtDb } from '@treecrdt/interface/sqlite';

export type SyncPendingOpsTable = {
  doc_id: string;
  op_ref: Uint8Array;
  op: Uint8Array;
  sig: Uint8Array;
  proof_ref: Uint8Array | null;
  reason: string;
  message: string | null;
  created_at_ms: number;
};

export type SyncOpAuthTable = {
  doc_id: string;
  op_ref: Uint8Array;
  sig: Uint8Array;
  proof_ref: Uint8Array | null;
  created_at_ms: number;
};

export type SyncCapabilityTable = {
  doc_id: string;
  name: string;
  value: string;
  created_at_ms: number;
};

/** Core TreeCRDT tables plus sync sidecar tables. */
export type TreecrdtSyncSqliteDb = TreecrdtDb & {
  treecrdt_sync_pending_ops: SyncPendingOpsTable;
  treecrdt_sync_op_auth: SyncOpAuthTable;
  treecrdt_sync_capability: SyncCapabilityTable;
};
