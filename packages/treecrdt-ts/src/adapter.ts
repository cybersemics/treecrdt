import type { Operation } from "./index.js";

export type SerializeNodeId = (id: string) => Uint8Array;
export type SerializeReplica = (replica: Operation["meta"]["id"]["replica"]) => Uint8Array;

export interface TreecrdtAdapter {
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
