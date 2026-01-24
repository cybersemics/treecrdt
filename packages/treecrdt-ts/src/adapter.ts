import type { Operation } from "./index.js";

export type SerializeNodeId = (id: string) => Uint8Array;
export type SerializeReplica = (replica: Operation["meta"]["id"]["replica"]) => Uint8Array;

export interface TreecrdtAdapter {
  /**
   * Set the document id used for v0 sync (`op_ref` derivation).
   *
   * This MUST be stable for the lifetime of the underlying storage, since it affects opRef hashes.
   */
  setDocId(docId: string): Promise<void> | void;
  /**
   * Return the currently configured document id (if supported).
   */
  docId(): Promise<string | null> | string | null;
  /**
   * Fetch all stored opRefs (16-byte values).
   *
   * Returns raw JSON-decoded values; `number[][]` (bytes) is the expected shape for SQLite-backed adapters.
   */
  opRefsAll(): Promise<unknown[]>;
  /**
   * Fetch opRefs relevant to the `children(parent)` filter.
   *
   * Returns raw JSON-decoded values; `number[][]` (bytes) is the expected shape for SQLite-backed adapters.
   */
  opRefsChildren(parent: Uint8Array): Promise<unknown[]>;
  /**
   * Fetch operations by opRef (16-byte values).
   *
   * Returns raw JSON-decoded operation rows (same shape as `opsSince`).
   */
  opsByOpRefs(opRefs: Uint8Array[]): Promise<unknown[]>;
  /**
   * Fetch materialized children for a parent node (16-byte id).
   *
   * Returns raw JSON-decoded values; `number[][]` (bytes) is the expected shape for SQLite-backed adapters.
   */
  treeChildren(parent: Uint8Array): Promise<unknown[]>;
  /**
   * Fetch materialized children for a parent node, with their stable ordering keys.
   *
   * This enables keyset pagination via `(order_key, node)` cursors.
   *
   * Returns raw JSON-decoded rows: `{ node: number[16], order_key: number[] | null }[]`.
   */
  treeChildrenPage?(
    parent: Uint8Array,
    cursor: { orderKey: Uint8Array; node: Uint8Array } | null,
    limit: number
  ): Promise<unknown[]>;
  /**
   * Dump the full materialized tree state.
   *
   * Returns raw JSON-decoded rows (array of objects with byte fields).
   */
  treeDump(): Promise<unknown[]>;
  /**
   * Count non-tombstoned nodes in the materialized tree (excluding ROOT).
   */
  treeNodeCount(): Promise<number> | number;
  /**
   * Fetch the maximum lamport seen in the op log.
   */
  headLamport(): Promise<number> | number;
  /**
   * Fetch the maximum counter observed for a replica id.
   */
  replicaMaxCounter(replica: Uint8Array): Promise<number> | number;
  /**
   * Append a single operation.
   */
  appendOp(
    op: Operation,
    serializeNodeId: SerializeNodeId,
    serializeReplica: SerializeReplica
  ): Promise<void> | void;
  /**
   * Optional batch append hook. When provided, callers can submit many ops
   * inside one transaction / prepared statement for better throughput.
   */
  appendOps?(
    ops: Operation[],
    serializeNodeId: SerializeNodeId,
    serializeReplica: SerializeReplica
  ): Promise<void> | void;
  opsSince(lamport: number, root?: string): Promise<unknown[]>;
  close?(): Promise<void> | void;
}

export type AdapterFactory<TConfig = void> = (
  config: TConfig
) => Promise<TreecrdtAdapter> | TreecrdtAdapter;
