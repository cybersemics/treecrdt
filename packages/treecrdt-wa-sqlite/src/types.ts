import type {
  MaterializationEvent,
  TreecrdtEngine,
  TreecrdtEngineTree,
} from '@treecrdt/interface/engine';
import { createMaterializationDispatcher } from '@treecrdt/interface/engine';
import type { Kysely, TreecrdtDb } from '@treecrdt/interface/sqlite';

/**
 * Open TreeCRDT SQLite connection: typed Kysely surface plus lifecycle.
 * Statement-level wa-sqlite APIs stay private inside createDatabase.
 */
export type Database = {
  readonly db: Kysely<TreecrdtDb>;
  /** Multi-statement SQL for extension schema init; prefer `db` for queries. */
  execBatch(sql: string): Promise<void>;
  close?: () => Promise<void> | void;
};

export type StorageMode = 'memory' | 'opfs';
export type ClientMode = 'direct' | 'worker';
export type RuntimeMode = 'direct' | 'dedicated-worker' | 'shared-worker';

export type TreecrdtClient = TreecrdtEngine & {
  mode: ClientMode;
  runtime: RuntimeMode;
  storage: StorageMode;
  /**
   * Type-safe SQL against TreeCRDT tables. Compiles on the caller; CRDT writes
   * still go through `ops` / `local` so the op log stays consistent.
   */
  db: Kysely<TreecrdtDb>;
  tree: Required<Pick<TreecrdtEngineTree, 'changedSince'>>;
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
