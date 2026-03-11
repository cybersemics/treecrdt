import { describe } from "vitest";
import { defineProofMaterialStoreContract } from "../../protocol/tests/helpers/proof-material-contract.ts";

import {
  createPostgresSyncCapabilityMaterialStore,
  createPostgresSyncOpAuthStore,
} from "../dist/index.js";

const POSTGRES_URL = process.env.TREECRDT_POSTGRES_URL ?? "postgresql://postgres:postgres@127.0.0.1:5432/postgres";
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
});
