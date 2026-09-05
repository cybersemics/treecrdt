import type { Remote } from 'comlink';
import type { TreecrdtConnection } from '../connection.js';
import type { TreecrdtSession } from '../session.js';
import type { OpenDbFn } from './direct.js';
import type { ClientMode, RuntimeMode, StorageMode } from '../types.js';

/**
 * Comlink remote of TreecrdtConnection with a nested session proxy.
 * Default Remote<> would type `session` as Promise because it is not ProxyMarked.
 */
export type RemoteTreecrdtConnection = Omit<Remote<TreecrdtConnection>, 'session'> & {
  readonly session: Remote<TreecrdtSession>;
};

/** In-process connection or Comlink remote of the same surface. */
export type ConnectionHandle = TreecrdtConnection | RemoteTreecrdtConnection;

export type RuntimeConnection = {
  connection: ConnectionHandle;
  mode: ClientMode;
  runtime: RuntimeMode;
  storage: StorageMode;
  filename: string;
  docId: string;
  /** False when `connection` is a Comlink remote (needs proxied materialization callbacks). */
  local: boolean;
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
