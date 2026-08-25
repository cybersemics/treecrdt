/// <reference lib="webworker" />
import * as Comlink from 'comlink';
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import {
  TreecrdtBackend,
  type BackendInitConfig,
  type BackendInitResult,
  type MaterializationListener,
} from './backend.js';
import { openTreecrdtDb } from './open.js';

type SharedWorkerGlobal = typeof globalThis & {
  onconnect: ((ev: MessageEvent) => void) | null;
};

type StoredConfig = {
  baseUrl: string;
  requestedFilename: string;
  requestedStorage: 'memory' | 'opfs';
  docId: string;
};

/** Forwards unknown properties to `backend`; overridden methods live on `overrides`. */
function decorateBackend<T extends object>(overrides: object, backend: TreecrdtBackend): T {
  return new Proxy(overrides, {
    get(target, prop, receiver) {
      if (Reflect.has(target, prop)) {
        const value = Reflect.get(target, prop, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      }
      const value = Reflect.get(backend, prop, backend);
      if (typeof value === 'function') return value.bind(backend);
      return value;
    },
  }) as T;
}

/**
 * One shared DB session with per-port Comlink endpoints.
 * Port bookkeeping (init lock, last-close, peer notify) stays here — not on TreecrdtBackend.
 */
class SharedWorkerHost {
  readonly backend = new TreecrdtBackend(openTreecrdtDb);
  private readonly ports = new Set<SharedWorkerPort>();
  private storedConfig: StoredConfig | null = null;
  private initResult: BackendInitResult | null = null;

  attach(port: MessagePort): void {
    const decorator = new SharedWorkerPort(this);
    this.ports.add(decorator);
    Comlink.expose(decorateBackend(decorator, this.backend), port);
    port.start();
  }

  async init(config: BackendInitConfig): Promise<BackendInitResult> {
    const requestedFilename =
      config.storage === 'opfs' ? (config.filename ?? '/treecrdt.db') : ':memory:';
    if (this.storedConfig && this.initResult) {
      const cfg = this.storedConfig;
      if (
        cfg.baseUrl !== (config.baseUrl ?? '/') ||
        cfg.requestedFilename !== requestedFilename ||
        cfg.requestedStorage !== config.storage ||
        cfg.docId !== config.docId
      ) {
        throw new Error('shared worker already initialized with a different TreeCRDT database');
      }
      return this.initResult;
    }

    const result = await this.backend.init({
      ...config,
      baseUrl: config.baseUrl ?? '/',
      opfsVfs: config.storage === 'opfs' ? 'any-context' : undefined,
    });
    this.storedConfig = {
      baseUrl: config.baseUrl ?? '/',
      requestedFilename,
      requestedStorage: config.storage,
      docId: config.docId,
    };
    this.initResult = result;
    return result;
  }

  async closePort(endpoint: SharedWorkerPort): Promise<void> {
    this.ports.delete(endpoint);
    endpoint.detachListener();
    if (this.ports.size > 0) return;
    await this.backend.close();
    this.storedConfig = null;
    this.initResult = null;
  }

  async dropPort(endpoint: SharedWorkerPort): Promise<void> {
    this.ports.delete(endpoint);
    endpoint.detachListener();
    await this.backend.drop();
    this.storedConfig = null;
    this.initResult = null;
  }

  notifyPeers(event: MaterializationEvent, exclude: MaterializationListener | null): void {
    this.backend.emitMaterialized(event, exclude ?? undefined);
  }
}

/** Per-port decorator: same backend API, with shared lifecycle + notify overrides only. */
class SharedWorkerPort {
  private listener: MaterializationListener | null = null;

  constructor(private readonly host: SharedWorkerHost) {}

  init(config: BackendInitConfig): Promise<BackendInitResult> {
    return this.host.init(config);
  }

  close(): Promise<void> {
    return this.host.closePort(this);
  }

  drop(): Promise<void> {
    return this.host.dropPort(this);
  }

  subscribeMaterialized(listener: MaterializationListener): void {
    this.detachListener();
    this.listener = listener;
    this.host.backend.subscribeMaterialized(listener);
  }

  unsubscribeMaterialized(listener: MaterializationListener): void {
    if (this.listener === listener) this.detachListener();
    else this.host.backend.unsubscribeMaterialized(listener);
  }

  /** Peer notify for client-side local writers (excludes this port's listener). */
  async notifyMaterialized(event: MaterializationEvent): Promise<void> {
    this.host.notifyPeers(event, this.listener);
  }

  detachListener(): void {
    if (!this.listener) return;
    this.host.backend.unsubscribeMaterialized(this.listener);
    this.listener = null;
  }
}

const host = new SharedWorkerHost();

(self as unknown as SharedWorkerGlobal).onconnect = (ev: MessageEvent) => {
  const port = ev.ports[0];
  if (!port) return;
  host.attach(port);
};
