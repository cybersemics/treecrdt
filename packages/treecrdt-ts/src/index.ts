export type ReplicaId = string | Uint8Array;
export type NodeId = string;
export type Lamport = number;

export type OperationId = {
  replica: ReplicaId;
  counter: number;
};

export type OperationMetadata = {
  id: OperationId;
  lamport: Lamport;
  // Optional defensive-deletion awareness payload for delete operations.
  // This is carried end-to-end as an opaque blob (JSON-encoded VersionVector in v0).
  knownState?: Uint8Array;
};

export type OperationKind =
  | {
      type: "insert";
      parent: NodeId;
      node: NodeId;
      position: number;
    }
  | {
      type: "move";
      node: NodeId;
      newParent: NodeId;
      position: number;
    }
  | {
      type: "delete";
      node: NodeId;
    }
  | {
      type: "tombstone";
      node: NodeId;
    };

export type Operation = {
  meta: OperationMetadata;
  kind: OperationKind;
};

export type Snapshot = {
  head: Lamport;
};

export type SubtreeFilter = {
  root: NodeId;
  depth?: number;
};

export interface AccessControl {
  canApply(op: Operation): Promise<void> | void;
  canRead(node: NodeId): Promise<void> | void;
}

export interface StorageAdapter {
  apply(op: Operation): Promise<void> | void;
  loadSince(lamport: Lamport): Promise<Operation[]> | Operation[];
  latestLamport(): Promise<Lamport> | Lamport;
  snapshot(): Promise<Snapshot> | Snapshot;
}

export interface TreeCRDT {
  insert(parent: NodeId, node: NodeId, position: number): Promise<Operation> | Operation;
  move(node: NodeId, newParent: NodeId, position: number): Promise<Operation> | Operation;
  delete(node: NodeId): Promise<Operation> | Operation;
  applyRemote(op: Operation): Promise<void> | void;
  operationsSince(lamport: Lamport, filter?: SubtreeFilter): Promise<Operation[]> | Operation[];
  snapshot(): Promise<Snapshot> | Snapshot;
}

export interface SyncProtocol {
  push(ops: Operation[]): Promise<void> | void;
  pull(since: Lamport, filter?: SubtreeFilter): Promise<Operation[]> | Operation[];
}

export * from "./adapter.js";
export * from "./ids.js";
