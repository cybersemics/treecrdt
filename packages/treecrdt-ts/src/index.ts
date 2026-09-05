// Replica identity for v0/v1 sync: Ed25519 public key bytes (32 bytes).
export type ReplicaId = Uint8Array;
export type NodeId = string;
export type Lamport = number;

export type OperationId = {
  replica: ReplicaId;
  counter: number;
};

export type OperationMetadata = {
  id: OperationId;
  lamport: Lamport;
  // Defensive-deletion awareness: absent for most kinds. Delete requires non-zero-length bytes;
  // the canonical empty VersionVector is a valid nine-byte value.
  // Carried end-to-end as an opaque canonical VersionVector v0 binary value.
  knownState?: Uint8Array;
};

export type OperationKind =
  | {
      type: 'insert';
      parent: NodeId;
      node: NodeId;
      /**
       * Stable sibling ordering key (LSEQ/Logoot-style).
       *
       * Stored/compared lexicographically; children(parent) must be ordered by (orderKey, nodeId).
       */
      orderKey: Uint8Array;
      /**
       * Optional application payload to initialize alongside insert.
       *
       * When present, this is treated like a `payload` op at the same `(lamport, replica, counter)`,
       * with last-writer-wins ordering per node.
       */
      payload?: Uint8Array;
    }
  | {
      type: 'move';
      node: NodeId;
      newParent: NodeId;
      /**
       * Stable sibling ordering key (LSEQ/Logoot-style).
       */
      orderKey: Uint8Array;
    }
  | {
      type: 'delete';
      node: NodeId;
    }
  | {
      type: 'tombstone';
      node: NodeId;
    }
  | {
      /**
       * Update opaque application payload for a node.
       *
       * Merge semantics are last-writer-wins per node, ordered by
       * `(lamport, replica, counter)`.
       */
      type: 'payload';
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

export * from './adapter.js';
export * from './ids.js';
export * from './engine.js';
