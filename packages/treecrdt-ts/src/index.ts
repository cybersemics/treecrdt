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
      /**
       * Optional application payload to initialize alongside insert.
       *
       * When present, this is treated like a `payload` op at the same `(lamport, replica, counter)`,
       * with last-writer-wins ordering per node.
       */
      payload?: Uint8Array;
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
    }
  | {
      /**
       * Update opaque application payload for a node.
       *
       * Merge semantics are last-writer-wins per node, ordered by
       * `(lamport, replica, counter)`.
       */
      type: "payload";
      node: NodeId;
      /**
       * `payload = Uint8Array` sets the value, `payload = null` clears it.
       */
      payload: Uint8Array | null;
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
  insert(parent: NodeId, node: NodeId, position: number, payload?: Uint8Array): Promise<Operation> | Operation;
  move(node: NodeId, newParent: NodeId, position: number): Promise<Operation> | Operation;
  delete(node: NodeId): Promise<Operation> | Operation;
  setPayload(node: NodeId, payload: Uint8Array | null): Promise<Operation> | Operation;
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
