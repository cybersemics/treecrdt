import type { Operation } from "@treecrdt/interface";
import type { TreecrdtSqlitePlacement } from "@treecrdt/interface/sqlite";

export type RpcStorageMode = "memory" | "opfs";

export type RpcSqlParam = number | string | null | Uint8Array;
export type RpcSqlParams = RpcSqlParam[];

export type RpcInitParams = {
  baseUrl: string;
  filename?: string;
  storage: RpcStorageMode;
  docId: string;
};

export type RpcInitResult = { storage: RpcStorageMode; opfsError?: string };

export type RpcSchema = {
  init: {
    params: [baseUrl: string, filename: string | undefined, storage: RpcStorageMode, docId: string];
    result: RpcInitResult;
  };
  sqlExec: { params: [sql: string]; result: void };
  sqlGetText: { params: [sql: string, params?: RpcSqlParams]; result: string | null };
  append: { params: [op: Operation]; result: void };
  appendMany: { params: [ops: Operation[]]; result: void };
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
  headLamport: { params: []; result: number };
  replicaMaxCounter: { params: [replica: number[]]; result: number };
  localInsert: {
    params: [replica: number[], parent: string, node: string, placement: TreecrdtSqlitePlacement, payload: Uint8Array | null];
    result: Operation;
  };
  localMove: {
    params: [replica: number[], node: string, newParent: string, placement: TreecrdtSqlitePlacement];
    result: Operation;
  };
  localDelete: { params: [replica: number[], node: string]; result: Operation };
  localPayload: { params: [replica: number[], node: string, payload: Uint8Array | null]; result: Operation };
  close: { params: []; result: void };
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
