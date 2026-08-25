import * as Comlink from 'comlink';
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import type { TreecrdtBackend } from '../backend.js';
import type { BackendInitConfig, BackendInitResult } from '../backend.js';
import type {
  ResolvedClientOptions,
  RuntimeConnection,
  RuntimeStrategy,
  SharedBackendHandle,
} from './types.js';

/** Comlink surface exposed per SharedWorker port (see shared-worker.ts). */
export type SharedWorkerPortApi = TreecrdtBackend & {
  notifyMaterialized(event: MaterializationEvent): Promise<void>;
};

export const sharedWorkerStrategy: RuntimeStrategy = {
  runtime: 'shared-worker',

  async connect(opts: ResolvedClientOptions): Promise<RuntimeConnection> {
    const name = opts.sharedWorkerName ?? `treecrdt:${opts.docId}:${opts.filename ?? '/treecrdt.db'}`;
    const sharedWorker = opts.workerUrl
      ? new SharedWorker(opts.workerUrl, { name, type: 'module' } as WorkerOptions & { name: string })
      : new SharedWorker(
          new URL('../shared-worker.js', import.meta.url),
          /* @vite-ignore */ { name, type: 'module' } as WorkerOptions & { name: string },
        );

    const port = sharedWorker.port;
    port.start();
    const backend = Comlink.wrap<SharedWorkerPortApi>(port) as SharedBackendHandle;
    let closed = false;

    const cleanup = async () => {
      if (closed) return;
      closed = true;
      try {
        (backend as Comlink.Remote<SharedWorkerPortApi>)[Comlink.releaseProxy]();
      } catch {
        // Proxy may already be released.
      }
      port.close();
    };

    const initConfig: BackendInitConfig = {
      baseUrl: opts.baseUrl ?? '/',
      filename: opts.filename,
      storage: opts.storage,
      docId: opts.docId,
      fallback: opts.fallback,
    };

    let initResult: BackendInitResult;
    try {
      initResult = await backend.init(initConfig);
    } catch (error) {
      try {
        await backend.close();
      } catch {
        // Initialization may not have completed; still detach the port.
      } finally {
        await cleanup();
      }
      throw error;
    }

    if (opts.fallback === 'throw' && initResult.storage !== 'opfs') {
      const reason = initResult.opfsError ? `: ${initResult.opfsError}` : '';
      try {
        await backend.close();
      } catch {
        // ignore close errors on init failure
      } finally {
        await cleanup();
      }
      throw new Error(`OPFS requested but could not be initialized${reason}`);
    }

    return {
      backend,
      mode: 'worker',
      runtime: 'shared-worker',
      storage: initResult.storage,
      filename: initResult.filename,
      docId: opts.docId,
      local: false,
      notifyPeers: (event) => backend.notifyMaterialized(event),
      dispose: cleanup,
    };
  },
};
