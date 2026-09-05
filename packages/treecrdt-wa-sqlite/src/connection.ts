import * as Comlink from 'comlink';
import type {
  BackendInitConfig,
  BackendInitResult,
  MaterializationListener,
  TreecrdtSession,
  TreecrdtSessionOwner,
} from './session.js';
import type { MaterializationEvent } from '@treecrdt/interface/engine';

/**
 * Runtime-specific attach policy: init/teardown, materialization subscription, peer notify.
 * Data access is always `connection.session` (same TreecrdtSession everywhere).
 */
export interface TreecrdtConnection {
  /** Open or join the session; returns storage/filename for RuntimeConnection metadata. */
  init: (config: BackendInitConfig) => Promise<BackendInitResult>;
  close: () => Promise<void>;
  drop: () => Promise<void>;
  subscribeMaterialized: (listener: MaterializationListener) => void;
  unsubscribeMaterialized: (listener: MaterializationListener) => void;
  /**
   * Forward client-side local-writer materialization to peers.
   * Exclusive: no-op (BroadcastChannel / single tab). Shared: fan-out excluding this port.
   */
  notifyMaterialized: (event: MaterializationEvent) => Promise<void>;
  /** Data API — local handle or Comlink-nested proxy of the same TreecrdtSession. */
  readonly session: TreecrdtSession;
}

/**
 * 1:1 client↔session lifecycle for direct and dedicated-worker runtimes.
 * session is Comlink.proxy'd so worker remotes nest instead of structured-cloning.
 */
const createExclusiveConnection = (owned: TreecrdtSessionOwner): TreecrdtConnection => ({
  session: Comlink.proxy(owned.session),
  init: owned.open,
  close: owned.closeDb,
  drop: owned.dropStorage,
  subscribeMaterialized: owned.subscribeMaterialized,
  unsubscribeMaterialized: owned.unsubscribeMaterialized,
  // No peer ports on this transport; OPFS cross-tab uses BroadcastChannel on the client.
  notifyMaterialized: async () => undefined,
});

export default createExclusiveConnection;
