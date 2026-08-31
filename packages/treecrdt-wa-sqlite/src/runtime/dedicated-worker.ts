import * as Comlink from 'comlink';
import type { BackendInitConfig, BackendInitResult } from '../session.js';
import type {
  RemoteTreecrdtConnection,
  ResolvedClientOptions,
  RuntimeConnection,
  RuntimeStrategy,
} from './types.js';

export const dedicatedWorkerStrategy: RuntimeStrategy = {
  runtime: 'dedicated-worker',

  async connect(opts: ResolvedClientOptions): Promise<RuntimeConnection> {
    const worker = (
      opts.workerUrl
        ? new Worker(opts.workerUrl, { type: 'module' })
        : new Worker(new URL('../worker.js', import.meta.url), { type: 'module' })
    ) as Worker;

    const connection = Comlink.wrap(worker) as unknown as RemoteTreecrdtConnection;
    let closed = false;

    const cleanup = async () => {
      if (closed) return;
      closed = true;
      try {
        connection[Comlink.releaseProxy]();
      } catch {
        // Proxy may already be released.
      }
      worker.terminate();
    };

    const initConfig: BackendInitConfig = {
      baseUrl: opts.baseUrl ?? '/',
      filename: opts.filename,
      storage: opts.storage,
      docId: opts.docId,
      fallback: opts.fallback,
    };

    // Comlink does not reject in-flight RPCs when the worker dies; race init
    // against Worker `error` so a load/crash cannot hang client creation.
    let rejectWorkerError!: (error: Error) => void;
    const workerError = new Promise<never>((_, reject) => {
      rejectWorkerError = reject;
    });
    const onWorkerError = (event: Event) => {
      const message =
        'message' in event && typeof event.message === 'string' && event.message.length > 0
          ? event.message
          : 'dedicated worker failed';
      rejectWorkerError(new Error(message));
    };
    worker.addEventListener('error', onWorkerError);

    let initResult: BackendInitResult;
    try {
      initResult = await Promise.race([connection.init(initConfig), workerError]);
    } catch (error) {
      await cleanup();
      throw error;
    } finally {
      worker.removeEventListener('error', onWorkerError);
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
      runtime: 'dedicated-worker',
      storage: initResult.storage === 'opfs' ? 'opfs' : 'memory',
      filename: initResult.filename,
      docId: opts.docId,
      local: false,
      dispose: cleanup,
    };
  },
};
