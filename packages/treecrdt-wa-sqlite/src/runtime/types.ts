import type { Remote } from 'comlink';
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import type { TreecrdtBackend } from '../backend.js';
import type { OpenDbFn } from './direct.js';
import type { ClientMode, RuntimeMode, StorageMode } from '../types.js';

/** In-process backend or Comlink remote of the same surface. */
export type BackendHandle = TreecrdtBackend | Remote<TreecrdtBackend>;

/**
 * Shared-worker port API: same data surface as TreecrdtBackend, plus peer notify for
 * client-side local writer materialization (adapter events already fan out via subscribe).
 */
export type SharedBackendHandle = BackendHandle & {
  notifyMaterialized(event: MaterializationEvent): Promise<void>;
};

export type RuntimeConnection = {
  backend: BackendHandle;
  mode: ClientMode;
  runtime: RuntimeMode;
  storage: StorageMode;
  filename: string;
  docId: string;
  /** False when `backend` is a Comlink remote (needs proxied materialization callbacks). */
  local: boolean;
  /** When set, local writer materialization is forwarded to peer tabs through the shared backend. */
  notifyPeers?: (event: MaterializationEvent) => Promise<void>;
  dispose: () => Promise<void>;
};

export type ResolvedClientOptions = {
  baseUrl?: string;
  filename?: string;
  storage: StorageMode;
  fallback: 'memory' | 'throw';
  requireOpfs: boolean;
  docId: string;
  workerUrl?: string | URL;
  sharedWorkerName?: string;
  openDb: OpenDbFn;
};

export type RuntimeStrategy = {
  readonly runtime: RuntimeMode;
  connect(opts: ResolvedClientOptions): Promise<RuntimeConnection>;
};
