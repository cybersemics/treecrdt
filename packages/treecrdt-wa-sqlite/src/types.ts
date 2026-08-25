import type { MaterializationEvent, TreecrdtEngine } from '@treecrdt/interface/engine';
import { createMaterializationDispatcher } from '@treecrdt/interface/engine';
import type { SqliteRunner } from '@treecrdt/interface/sqlite';

/**
 * Open wa-sqlite handle: statement APIs + SqliteRunner (exec/getText).
 * Pass directly to createTreecrdtSqliteAdapter — no runner wrapper.
 */
export type Database = {
  prepare(sql: string): Promise<number> | number;
  bind(stmt: number, index: number, value: unknown): Promise<void> | void;
  step(stmt: number): Promise<number> | number;
  column_text(stmt: number, index: number): Promise<string> | string;
  finalize(stmt: number): Promise<void> | void;
  close?(): Promise<void> | void;
} & SqliteRunner;

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
  /** Browser: public URL prefix for wa-sqlite JS assets. Node: optional filesystem directory. */
  baseUrl?: string;
};

export type TreecrdtClient = TreecrdtEngine & {
  mode: ClientMode;
  runtime: RuntimeMode;
  storage: StorageMode;
  runner: SqliteRunner;
  drop: () => Promise<void>;
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
