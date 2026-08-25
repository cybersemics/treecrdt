/// <reference lib="webworker" />
import * as Comlink from 'comlink';
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import type { TreecrdtConnection } from './connection.js';
import type {
  BackendInitConfig,
  BackendInitResult,
  MaterializationListener,
  TreecrdtSessionOwner,
} from './session.js';
import createTreecrdtSession from './session.js';
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

type SharedPortConnection = TreecrdtConnection & {
  detachListener: () => void;
};

type SharedConnectionHost = {
  owner: TreecrdtSessionOwner;
  init: (config: BackendInitConfig) => Promise<BackendInitResult>;
  closePort: (endpoint: SharedPortConnection) => Promise<void>;
  dropPort: (endpoint: SharedPortConnection) => Promise<void>;
};

/**
 * Per-port connection: N tabs share one session via the host.
 * session is Comlink.proxy'd — no runtime Proxy decoration of the wire surface.
 */
const createSharedConnection = (host: SharedConnectionHost): SharedPortConnection => {
  let listener: MaterializationListener | null = null;

  /** Remove this port's materialization listener if subscribed. */
  const detachListener = (): void => {
    if (!listener) return;
    host.owner.unsubscribeMaterialized(listener);
    listener = null;
  };

  const connection: SharedPortConnection = {
    session: Comlink.proxy(host.owner.session),
    init: (config) => host.init(config),
    close: () => host.closePort(connection),
    drop: () => host.dropPort(connection),
    subscribeMaterialized: (next) => {
      detachListener();
      listener = next;
      host.owner.subscribeMaterialized(next);
    },
    unsubscribeMaterialized: (next) => {
      if (listener === next) detachListener();
      else host.owner.unsubscribeMaterialized(next);
    },
    /** Peer notify for client-side local writers (excludes this port's listener). */
    notifyMaterialized: async (event: MaterializationEvent) => {
      host.owner.emitMaterialized(event, listener ?? undefined);
    },
    detachListener,
  };

  return connection;
};

/**
 * One shared DB session with per-port Comlink endpoints.
 * Init lock, last-close, and peer notify live here — not on TreecrdtSession.
 */
const createSharedWorkerHost = () => {
  const owner = createTreecrdtSession(openTreecrdtDb);
  const ports = new Set<SharedPortConnection>();
  let storedConfig: StoredConfig | null = null;
  let initResult: BackendInitResult | null = null;

  /** Join an existing session or open once; rejects mismatched configs. */
  const init = async (config: BackendInitConfig): Promise<BackendInitResult> => {
    const requestedFilename =
      config.storage === 'opfs' ? (config.filename ?? '/treecrdt.db') : ':memory:';
    if (storedConfig && initResult) {
      const cfg = storedConfig;
      if (
        cfg.baseUrl !== (config.baseUrl ?? '/') ||
        cfg.requestedFilename !== requestedFilename ||
        cfg.requestedStorage !== config.storage ||
        cfg.docId !== config.docId
      ) {
        throw new Error('shared worker already initialized with a different TreeCRDT database');
      }
      return initResult;
    }

    const result = await owner.open({
      ...config,
      baseUrl: config.baseUrl ?? '/',
      opfsVfs: config.storage === 'opfs' ? 'any-context' : undefined,
    });
    storedConfig = {
      baseUrl: config.baseUrl ?? '/',
      requestedFilename,
      requestedStorage: config.storage,
      docId: config.docId,
    };
    initResult = result;
    return result;
  };

  /** Detach a port; close the session when the last port leaves. */
  const closePort = async (endpoint: SharedPortConnection): Promise<void> => {
    ports.delete(endpoint);
    endpoint.detachListener();
    if (ports.size > 0) return;
    await owner.closeDb();
    storedConfig = null;
    initResult = null;
  };

  /** Detach a port and drop durable storage for every peer. */
  const dropPort = async (endpoint: SharedPortConnection): Promise<void> => {
    ports.delete(endpoint);
    endpoint.detachListener();
    await owner.dropStorage();
    storedConfig = null;
    initResult = null;
  };

  /** Expose a new SharedConnection on this MessagePort. */
  const attach = (port: MessagePort): void => {
    const connection = createSharedConnection({
      owner,
      init,
      closePort,
      dropPort,
    });
    ports.add(connection);
    Comlink.expose(connection, port);
    port.start();
  };

  return { attach };
};

const host = createSharedWorkerHost();

(self as unknown as SharedWorkerGlobal).onconnect = (ev: MessageEvent) => {
  const port = ev.ports[0];
  if (!port) return;
  host.attach(port);
};
