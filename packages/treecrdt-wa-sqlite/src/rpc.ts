import type { Operation } from '@treecrdt/interface';
import type { MaterializationEvent, MaterializationOutcome } from '@treecrdt/interface/engine';

export type RpcStorageMode = 'memory' | 'opfs';
export type RpcOpfsWriteMode = 'default' | 'single-owner-wal' | 'opfs-write-ahead';

export type RpcSqlParam = number | string | null | Uint8Array;
export type RpcSqlParams = RpcSqlParam[];

export type RpcInitParams = {
  baseUrl: string;
  filename?: string;
  storage: RpcStorageMode;
  docId: string;
  opfsWriteMode?: RpcOpfsWriteMode;
};

export type RpcInitResult = { storage: RpcStorageMode; filename: string; opfsError?: string };

export type RpcSchema = {
  init: {
    params: [
      baseUrl: string,
      filename: string | undefined,
      storage: RpcStorageMode,
      docId: string,
      opfsWriteMode?: RpcOpfsWriteMode,
    ];
    result: RpcInitResult;
  };
  sqlExec: { params: [sql: string]; result: void };
  sqlGetText: { params: [sql: string, params?: RpcSqlParams]; result: string | null };
  append: { params: [op: Operation]; result: MaterializationOutcome };
  appendMany: {
    params: [ops: Operation[]];
    result: MaterializationOutcome;
  };
  broadcastMaterialized: { params: [event: MaterializationEvent]; result: void };
  opsSince: { params: [lamport: number, root?: string]; result: unknown[] };
  opRefsAll: { params: []; result: unknown[] };
  opRefsChildren: { params: [parent: string]; result: unknown[] };
  opsByOpRefs: { params: [opRefs: number[][]]; result: unknown[] };
  treeChildren: { params: [parent: string]; result: unknown[] };
  treeChildrenPage: {
    params: [parent: string, cursor: { orderKey: number[]; node: number[] } | null, limit: number];
    result: unknown[];
  };
  treeDump: { params: []; result: unknown[] };
  treeNodeCount: { params: []; result: number };
  treeParent: { params: [node: string]; result: Uint8Array | null };
  treeExists: { params: [node: string]; result: boolean };
  treePayload: { params: [node: string]; result: Uint8Array | null };
  headLamport: { params: []; result: number };
  replicaMaxCounter: { params: [replica: number[]]; result: number };
  close: { params: []; result: void };
  drop: { params: []; result: void };
};

export type RpcMethod = keyof RpcSchema;
export type RpcParams<M extends RpcMethod> = RpcSchema[M]['params'];
export type RpcResult<M extends RpcMethod> = RpcSchema[M]['result'];

export type RpcRequest<M extends RpcMethod = RpcMethod> = {
  id: number;
  method: M;
  params: RpcParams<M>;
};

export type RpcResponse<M extends RpcMethod = RpcMethod> =
  | { id: number; ok: true; result: RpcResult<M> }
  | { id: number; ok: false; error: string };

export type RpcPushMessage = {
  type: 'materialized';
  event: MaterializationEvent;
};

export function rpcBinaryResult(bytes: Uint8Array | null): Uint8Array | null {
  if (bytes === null) return null;
  const buffer = bytes.buffer;
  if (
    buffer instanceof ArrayBuffer &&
    bytes.byteOffset === 0 &&
    bytes.byteLength === buffer.byteLength
  ) {
    return bytes;
  }
  return Uint8Array.from(bytes);
}

export function transferablesForRpcBinaryResult(result: unknown): Transferable[] {
  if (!(result instanceof Uint8Array)) return [];
  const buffer = result.buffer;
  if (
    buffer instanceof ArrayBuffer &&
    result.byteOffset === 0 &&
    result.byteLength === buffer.byteLength
  ) {
    return [buffer];
  }
  return [];
}
