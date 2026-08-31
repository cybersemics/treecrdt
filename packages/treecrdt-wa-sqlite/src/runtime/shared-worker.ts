import * as Comlink from 'comlink';
import type { BackendInitConfig, BackendInitResult } from '../session.js';
import type {
  RemoteTreecrdtConnection,
  ResolvedClientOptions,
  RuntimeConnection,
  RuntimeStrategy,
} from './types.js';

export const sharedWorkerStrategy: RuntimeStrategy = {
  runtime: 'shared-worker',

  async connect(opts: ResolvedClientOptions): Promise<RuntimeConnection> {
    const name =
      opts.sharedWorkerName ?? `treecrdt:${opts.docId}:${opts.filename ?? '/treecrdt.db'}`;
    const sharedWorker = opts.workerUrl
      ? new SharedWorker(opts.workerUrl, { name, type: 'module' } as WorkerOptions & {
          name: string;
        })
      : new SharedWorker(
          new URL('../shared-worker.js', import.meta.url),
          /* @vite-ignore */ { name, type: 'module' } as WorkerOptions & { name: string },
        );

    const port = sharedWorker.port;
    port.start();
    const connection = Comlink.wrap(port) as unknown as RemoteTreecrdtConnection;
    let closed = false;

    const cleanup = async () => {
      if (closed) return;
      closed = true;
      try {
        connection[Comlink.releaseProxy]();
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

    // Comlink does not reject in-flight RPCs when the worker dies; race init
    // against SharedWorker `error` so a load/crash cannot hang client creation.
    let rejectWorkerError!: (error: Error) => void;
    const workerError = new Promise<never>((_, reject) => {
      rejectWorkerError = reject;
    });
    let workerCrashed = false;
    const onWorkerError = (event: Event) => {
      workerCrashed = true;
      const message =
        'message' in event && typeof event.message === 'string' && event.message.length > 0
          ? event.message
          : 'shared worker failed';
      rejectWorkerError(new Error(message));
    };
    sharedWorker.addEventListener('error', onWorkerError);

    let initResult: BackendInitResult;
    try {
      initResult = await Promise.race([connection.init(initConfig), workerError]);
    } catch (error) {
      // A dead worker cannot answer close(); only detach the local port.
      if (!workerCrashed) {
        try {
          await connection.close();
        } catch {
          // Initialization may not have completed; still detach the port.
        }
      }
      await cleanup();
      throw error;
    } finally {
      sharedWorker.removeEventListener('error', onWorkerError);
    }

    if (opts.fallback === 'throw' && initResult.storage !== 'opfs') {
      const reason = initResult.opfsError ? `: ${initResult.opfsError}` : '';
      try {
        await connection.close();
      } catch {
        // ignore close errors on init failure
      } finally {
        await cleanup();
      }
      throw new Error(`OPFS requested but could not be initialized${reason}`);
    }

    return {
      connection,
      mode: 'worker',
      runtime: 'shared-worker',
      storage: initResult.storage,
      filename: initResult.filename,
      docId: opts.docId,
      local: false,
      dispose: cleanup,
    };
  },
};
