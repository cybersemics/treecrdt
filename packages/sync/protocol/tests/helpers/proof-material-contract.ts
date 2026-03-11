import { randomUUID } from "node:crypto";

import { expect, test } from "vitest";

import type {
  Capability,
  OpAuth,
  OpRef,
  SyncCapabilityMaterialStore,
  SyncOpAuthStore,
} from "@treecrdt/sync";

type ProofMaterialDocStores = {
  opAuth: SyncOpAuthStore;
  capabilities: SyncCapabilityMaterialStore;
};

type ProofMaterialHarness = {
  createDocStores: (docId: string) => Promise<ProofMaterialDocStores> | ProofMaterialDocStores;
  close?: () => Promise<void> | void;
};

function makeOpRef(fill: number): OpRef {
  return new Uint8Array(16).fill(fill);
}

function makeOpAuth(sigFill: number, proofFill?: number): OpAuth {
  return {
    sig: new Uint8Array(64).fill(sigFill),
    ...(proofFill === undefined ? {} : { proofRef: new Uint8Array(16).fill(proofFill) }),
  };
}

function sortCapabilities(caps: Capability[]): Capability[] {
  return [...caps].sort((a, b) => `${a.name}:${a.value}`.localeCompare(`${b.name}:${b.value}`));
}

function normalizeOpAuth(value: OpAuth): OpAuth {
  return {
    sig: Uint8Array.from(value.sig),
    ...(value.proofRef ? { proofRef: Uint8Array.from(value.proofRef) } : {}),
  };
}

function normalizeOpAuthList(values: Array<OpAuth | null>): Array<OpAuth | null> {
  return values.map((value) => (value ? normalizeOpAuth(value) : null));
}

export function defineProofMaterialStoreContract(
  label: string,
  createHarness: () => Promise<ProofMaterialHarness> | ProofMaterialHarness
): void {
  test(`${label}: op auth round-trips in request order`, async () => {
    const harness = await createHarness();
    try {
      const { opAuth } = await harness.createDocStores(`doc-op-auth-${randomUUID()}`);
      const refA = makeOpRef(1);
      const refB = makeOpRef(2);
      const missing = makeOpRef(3);
      const authA = makeOpAuth(9, 4);
      const authB = makeOpAuth(7);

      await opAuth.storeOpAuth([
        { opRef: refA, auth: authA },
        { opRef: refB, auth: authB },
      ]);

      expect(normalizeOpAuthList(await opAuth.getOpAuthByOpRefs([refB, missing, refA]))).toEqual([authB, null, authA]);
    } finally {
      await harness.close?.();
    }
  });

  test(`${label}: op auth updates do not leak across docs`, async () => {
    const harness = await createHarness();
    try {
      const sharedRef = makeOpRef(5);
      const docA = await harness.createDocStores(`doc-a-${randomUUID()}`);
      const docB = await harness.createDocStores(`doc-b-${randomUUID()}`);
      const authA1 = makeOpAuth(1, 2);
      const authA2 = makeOpAuth(3, 4);
      const authB = makeOpAuth(8);

      await docA.opAuth.storeOpAuth([{ opRef: sharedRef, auth: authA1 }]);
      await docA.opAuth.storeOpAuth([{ opRef: sharedRef, auth: authA2 }]);
      await docB.opAuth.storeOpAuth([{ opRef: sharedRef, auth: authB }]);

      expect(normalizeOpAuthList(await docA.opAuth.getOpAuthByOpRefs([sharedRef]))).toEqual([authA2]);
      expect(normalizeOpAuthList(await docB.opAuth.getOpAuthByOpRefs([sharedRef]))).toEqual([authB]);
    } finally {
      await harness.close?.();
    }
  });

  test(`${label}: capabilities dedupe without leaking across docs`, async () => {
    const harness = await createHarness();
    try {
      const docA = await harness.createDocStores(`doc-cap-a-${randomUUID()}`);
      const docB = await harness.createDocStores(`doc-cap-b-${randomUUID()}`);
      const capA1: Capability = { name: "auth.capability", value: "token-a1" };
      const capA2: Capability = { name: "auth.capability", value: "token-a2" };
      const capB: Capability = { name: "auth.capability", value: "token-b" };

      await docA.capabilities.storeCapabilities([capA1, capA2, capA1]);
      await docB.capabilities.storeCapabilities([capB]);

      await expect(docA.capabilities.listCapabilities()).resolves.toEqual(sortCapabilities([capA1, capA2]));
      await expect(docB.capabilities.listCapabilities()).resolves.toEqual([capB]);
    } finally {
      await harness.close?.();
    }
  });
}
