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

    let initResult: BackendInitResult;
    try {
      initResult = await connection.init(initConfig);
    } catch (error) {
      await cleanup();
      throw error;
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
