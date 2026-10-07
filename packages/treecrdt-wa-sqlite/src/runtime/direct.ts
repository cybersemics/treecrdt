import type { OpenTreecrdtDbOptions, OpenTreecrdtDbResult } from '../open-core.js';
import createExclusiveConnection from '../connection.js';
import createTreecrdtSession from '../session.js';
import type { ResolvedClientOptions, RuntimeConnection, RuntimeStrategy } from './types.js';

export type OpenDbFn = (opts: OpenTreecrdtDbOptions) => Promise<OpenTreecrdtDbResult>;

export const directRuntimeStrategy: RuntimeStrategy = {
  runtime: 'direct',

  async connect(opts: ResolvedClientOptions): Promise<RuntimeConnection> {
    const connection = createExclusiveConnection(createTreecrdtSession(opts.openDb));
    const initResult = await connection.init({
      baseUrl: opts.baseUrl,
      filename: opts.filename,
      storage: opts.storage,
      docId: opts.docId,
      // Direct OPFS uses any-context so it can run off the dedicated worker origin.
      opfsVfs: opts.storage === 'opfs' ? 'any-context' : undefined,
    });

    return {
      connection,
      mode: 'direct',
      runtime: 'direct',
      storage: initResult.storage,
      filename: initResult.filename,
      docId: opts.docId,
      local: true,
      dispose: async () => {
        // close/drop on the public client already tear down the session.
      },
    };
  },
};
