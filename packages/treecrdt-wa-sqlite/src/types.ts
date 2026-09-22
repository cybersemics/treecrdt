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

export type TreecrdtClient = TreecrdtEngine & {
  mode: ClientMode;
  runtime: RuntimeMode;
  storage: StorageMode;
  runner: SqliteRunner;
  drop: () => Promise<void>;
};

export type ClientOptions = {
  /** Stable logical document identity used by CRDT operations and sync; cannot be detected. */
  docId: string;
  /** true → durable OPFS storage (throws when OPFS is unavailable); false/omitted → in-memory. */
  persistent?: boolean;
  /** OPFS database filename override; used only when persistent is true. */
  filename?: string;
  /** Share one worker and database session between browser tabs. */
  crossTab?: boolean;
  /** Browser-only public URL prefix containing the wa-sqlite JavaScript assets. */
  assetsBaseUrl?: string;
};

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
