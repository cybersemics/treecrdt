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
    const worker = new Worker(new URL('../worker.js', import.meta.url), { type: 'module' });

    const connection = Comlink.wrap(worker) as unknown as RemoteTreecrdtConnection;

    const cleanup = async () => {
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
    };

    let initResult: BackendInitResult;
    try {
      initResult = await connection.init(initConfig);
    } catch (error) {
      await cleanup();
      throw error;
    }

    return {
      connection,
      mode: 'worker',
      runtime: 'dedicated-worker',
      storage: initResult.storage,
      filename: initResult.filename,
      docId: opts.docId,
      local: false,
      dispose: cleanup,
    };
  },
};
