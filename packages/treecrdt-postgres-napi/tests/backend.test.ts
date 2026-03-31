import { describe } from 'vitest';

import { defineSyncBackendContract } from '../../sync/protocol/tests/helpers/sync-backend-contract.ts';

import { createPostgresNapiSyncBackendFactory } from '../dist/index.js';

const POSTGRES_URL = process.env.TREECRDT_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;

maybeDescribe('postgres-napi sync backend contract', () => {
  let factoryPromise: Promise<ReturnType<typeof createPostgresNapiSyncBackendFactory>> | undefined;

  const getFactory = async () => {
    if (!factoryPromise) {
      factoryPromise = (async () => {
        const factory = createPostgresNapiSyncBackendFactory(POSTGRES_URL!);
        await factory.ensureSchema();
        return factory;
      })();
    }
    return await factoryPromise;
  };

  defineSyncBackendContract('postgres-napi sync backend', async () => {
    const factory = await getFactory();
    return {
      openBackend: async (docId) => await factory.open(docId),
    };
  });
});
