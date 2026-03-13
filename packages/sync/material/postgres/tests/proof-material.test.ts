import { describe } from "vitest";
import { defineProofMaterialStoreContract } from "../../protocol/tests/helpers/proof-material-contract.ts";
import { definePendingProofMaterialStoreContract } from "../../protocol/tests/helpers/pending-proof-material-contract.ts";
import { defineReplayOnlyAuthStoreContract } from "../../protocol/tests/helpers/replay-only-auth-contract.ts";

import {
  createPostgresSyncCapabilityMaterialStore,
  createPostgresSyncOpAuthStore,
  createPostgresSyncPendingOpsStore,
} from "../dist/index.js";

const POSTGRES_URL = process.env.TREECRDT_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;

maybeDescribe("postgres proof material stores", () => {
  defineProofMaterialStoreContract("postgres proof material stores", async () => {
    const opAuthStore = createPostgresSyncOpAuthStore({ postgresUrl: POSTGRES_URL! });
    const capabilityStore = createPostgresSyncCapabilityMaterialStore({ postgresUrl: POSTGRES_URL! });
    await opAuthStore.init();
    await capabilityStore.init();

    return {
      createDocStores: async (docId) => ({
        opAuth: opAuthStore.forDoc(docId),
        capabilities: capabilityStore.forDoc(docId),
      }),
      close: async () => {
        await Promise.all([opAuthStore.close(), capabilityStore.close()]);
      },
    };
  });

  definePendingProofMaterialStoreContract("postgres pending proof material stores", async () => {
    const pendingStore = createPostgresSyncPendingOpsStore({ postgresUrl: POSTGRES_URL! });
    await pendingStore.init();

    return {
      createPendingStore: async (docId) => pendingStore.forDoc(docId),
      close: async () => {
        await pendingStore.close();
      },
    };
  });

  defineReplayOnlyAuthStoreContract("postgres replay-only auth material stores", async () => {
    const opAuthStore = createPostgresSyncOpAuthStore({ postgresUrl: POSTGRES_URL! });
    const capabilityStore = createPostgresSyncCapabilityMaterialStore({ postgresUrl: POSTGRES_URL! });
    await opAuthStore.init();
    await capabilityStore.init();

    return {
      createDocStores: async (docId) => ({
        opAuth: opAuthStore.forDoc(docId),
        capabilities: capabilityStore.forDoc(docId),
      }),
      close: async () => {
        await Promise.all([opAuthStore.close(), capabilityStore.close()]);
      },
    };
  });
});
