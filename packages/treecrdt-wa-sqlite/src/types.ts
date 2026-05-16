import type { MaterializationEvent, TreecrdtEngine } from '@treecrdt/interface/engine';
import { createMaterializationDispatcher } from '@treecrdt/interface/engine';
import type { SqliteRunner } from '@treecrdt/interface/sqlite';
import type { TreecrdtSqliteAuthApi } from '@treecrdt/sync-sqlite/auth';
import type { RpcMethod, RpcParams, RpcRequest, RpcResult } from './rpc.js';

// Minimal wa-sqlite surface needed by the adapter. Exported so consumers
// don't need to import types from wa-sqlite directly.
export type Database = {
  prepare(sql: string): Promise<number> | number;
  bind(stmt: number, index: number, value: unknown): Promise<void> | void;
  step(stmt: number): Promise<number> | number;
  column_text(stmt: number, index: number): Promise<string> | string;
  finalize(stmt: number): Promise<void> | void;
  exec(sql: string): Promise<void> | void;
  close?(): Promise<void> | void;
};

export type StorageMode = 'memory' | 'opfs';
export type ClientMode = 'direct' | 'worker';
export type RuntimeMode = 'direct' | 'dedicated-worker' | 'shared-worker';
export type TreecrdtStorage =
  | { type: 'memory' }
  | { type: 'opfs'; filename?: string; fallback?: 'throw' | 'memory' }
  | { type: 'auto'; filename?: string; fallback?: 'memory' | 'throw' };
export type TreecrdtRuntime =
  | { type: 'auto' }
  | { type: 'direct' }
  | { type: 'dedicated-worker'; workerUrl?: string | URL }
  | { type: 'shared-worker'; workerUrl?: string | URL; name?: string };
export type TreecrdtAssets = {
  baseUrl?: string;
};

export type TreecrdtClient = TreecrdtEngine & {
  mode: ClientMode;
  runtime: RuntimeMode;
  storage: StorageMode;
  runner: SqliteRunner;
  auth: TreecrdtClientAuthApi;
  drop: () => Promise<void>;
};

export type TreecrdtSqliteAuthModule = typeof import('@treecrdt/sync-sqlite/auth');

export type TreecrdtClientAuthApi = {
  createSession: (
    ...args: Parameters<TreecrdtSqliteAuthApi['createSession']>
  ) => Promise<ReturnType<TreecrdtSqliteAuthApi['createSession']>>;
  describeCapabilityToken: TreecrdtSqliteAuthApi['describeCapabilityToken'];
  evaluateScope: (
    ...args: Parameters<TreecrdtSqliteAuthApi['evaluateScope']>
  ) => Promise<Awaited<ReturnType<TreecrdtSqliteAuthApi['evaluateScope']>>>;
};

export type ClientOptions = {
  storage?: TreecrdtStorage;
  runtime?: TreecrdtRuntime;
  assets?: TreecrdtAssets;
  docId?: string; // used for v0 sync opRef derivation inside the extension
};

export type NormalizedStorageOptions = {
  type: StorageMode | 'auto';
  filename?: string;
  requireOpfs: boolean;
  fallback: 'memory' | 'throw';
};

export type NormalizedRuntimeOptions = TreecrdtRuntime;

export type WorkerProxy = {
  postMessage(msg: RpcRequest, transfer?: Transferable[]): void;
  terminate: () => void;
  addEventListener: (type: 'message' | 'error', fn: (ev: any) => void) => void;
  removeEventListener: (type: 'message' | 'error', fn: (ev: any) => void) => void;
};

export type MessagePortProxy = {
  postMessage(msg: RpcRequest, transfer?: Transferable[]): void;
  start: () => void;
  close: () => void;
  addEventListener: (type: 'message' | 'messageerror', fn: (ev: any) => void) => void;
  removeEventListener: (type: 'message' | 'messageerror', fn: (ev: any) => void) => void;
};

export type RpcCall = <M extends RpcMethod>(method: M, params: RpcParams<M>) => Promise<RpcResult<M>>;
export type SharedWorkerFactory = (options?: WorkerOptions & { name?: string }) => SharedWorker;
export type CrossTabMaterializationScope = {
  docId: string;
  filename: string;
};
export type CrossTabMaterializationMessage = {
  type: 'treecrdt-materialized-v1';
  sourceId: string;
  docId: string;
  filename: string;
  event: MaterializationEvent;
};
export type ClientMaterializationDispatcherOptions = {
  broadcast?: (event: MaterializationEvent) => void;
};
export type ClientMaterializationDispatcher = ReturnType<typeof createMaterializationDispatcher> & {
  enableCrossTab: (scope: CrossTabMaterializationScope) => void;
  emitIncomingEvent: (event: MaterializationEvent) => void;
  close: () => void;
};
