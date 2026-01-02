import type { Operation } from "@treecrdt/interface";

export type RpcStorageMode = "memory" | "opfs";

export type RpcInitParams = {
  baseUrl: string;
  filename?: string;
  storage: RpcStorageMode;
  docId: string;
};

export type RpcInitResult = { storage: RpcStorageMode; opfsError?: string };

export type RpcSchema = {
  init: { params: RpcInitParams; result: RpcInitResult };
  append: { params: { op: Operation }; result: void };
  appendMany: { params: { ops: Operation[] }; result: void };
  opsSince: { params: { lamport: number; root?: string }; result: unknown[] };
  opRefsAll: { params?: undefined; result: unknown[] };
  opRefsChildren: { params: { parent: string }; result: unknown[] };
  opsByOpRefs: { params: { opRefs: number[][] }; result: unknown[] };
  treeChildren: { params: { parent: string }; result: unknown[] };
  treeDump: { params?: undefined; result: unknown[] };
  treeNodeCount: { params?: undefined; result: number };
  headLamport: { params?: undefined; result: number };
  replicaMaxCounter: { params: { replica: number[] | string }; result: number };
  close: { params?: undefined; result: void };
};

export type RpcMethod = keyof RpcSchema;
export type RpcParams<M extends RpcMethod> = RpcSchema[M]["params"];
export type RpcResult<M extends RpcMethod> = RpcSchema[M]["result"];

export type RpcRequest<M extends RpcMethod = RpcMethod> = {
  id: number;
  method: M;
  params: RpcParams<M>;
};

export type RpcResponse<M extends RpcMethod = RpcMethod> =
  | { id: number; ok: true; result: RpcResult<M> }
  | { id: number; ok: false; error: string };
