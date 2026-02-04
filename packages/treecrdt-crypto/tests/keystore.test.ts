import { expect, test } from "vitest";

import {
  generateTreecrdtDeviceWrapKeyV1,
  generateTreecrdtDocPayloadKeyV1,
  generateTreecrdtDocKeyBundleV1,
  generateTreecrdtIssuerKeyV1,
  generateTreecrdtLocalIdentityV1,
  openTreecrdtDocKeyBundleV1,
  openTreecrdtDocPayloadKeyV1,
  openTreecrdtIssuerKeyV1,
  openTreecrdtLocalIdentityV1,
  sealTreecrdtDocKeyBundleV1,
  sealTreecrdtDocPayloadKeyV1,
  sealTreecrdtIssuerKeyV1,
  sealTreecrdtLocalIdentityV1,
} from "../dist/keystore.js";

function bytesToHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("hex");
}

test("keystore v1: seal/open doc key bundle roundtrip", async () => {
  const docId = "doc-keys-1";
  const wrapKey = generateTreecrdtDeviceWrapKeyV1();
  const bundle = await generateTreecrdtDocKeyBundleV1({ docId });

  const sealed = await sealTreecrdtDocKeyBundleV1({ wrapKey, bundle });
  const opened = await openTreecrdtDocKeyBundleV1({ wrapKey, docId, sealed });

  expect(opened.docId).toBe(docId);
  expect(bytesToHex(opened.issuerSk)).toBe(bytesToHex(bundle.issuerSk));
  expect(bytesToHex(opened.replicaSk)).toBe(bytesToHex(bundle.replicaSk));
  expect(bytesToHex(opened.issuerPk)).toBe(bytesToHex(bundle.issuerPk));
  expect(bytesToHex(opened.replicaPk)).toBe(bytesToHex(bundle.replicaPk));
});

test("keystore v1: open fails with wrong docId (AAD mismatch)", async () => {
  const wrapKey = generateTreecrdtDeviceWrapKeyV1();
  const bundle = await generateTreecrdtDocKeyBundleV1({ docId: "doc-a" });
  const sealed = await sealTreecrdtDocKeyBundleV1({ wrapKey, bundle });

  await expect(openTreecrdtDocKeyBundleV1({ wrapKey, docId: "doc-b", sealed })).rejects.toThrow();
});

test("keystore v1: seal/open doc payload key roundtrip", async () => {
  const docId = "doc-payload-key-1";
  const wrapKey = generateTreecrdtDeviceWrapKeyV1();
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });

  const sealed = await sealTreecrdtDocPayloadKeyV1({ wrapKey, docId, payloadKey });
  const opened = await openTreecrdtDocPayloadKeyV1({ wrapKey, docId, sealed });

  expect(opened.docId).toBe(docId);
  expect(bytesToHex(opened.payloadKey)).toBe(bytesToHex(payloadKey));
});

test("keystore v1: open doc payload key fails with wrong docId (AAD mismatch)", async () => {
  const wrapKey = generateTreecrdtDeviceWrapKeyV1();
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId: "doc-a" });
  const sealed = await sealTreecrdtDocPayloadKeyV1({ wrapKey, docId: "doc-a", payloadKey });

  await expect(openTreecrdtDocPayloadKeyV1({ wrapKey, docId: "doc-b", sealed })).rejects.toThrow();
});

test("keystore v1: per-doc bundles are unlinkable by default (distinct keys)", async () => {
  const wrapKey = generateTreecrdtDeviceWrapKeyV1();
  const a = await generateTreecrdtDocKeyBundleV1({ docId: "doc-a" });
  const b = await generateTreecrdtDocKeyBundleV1({ docId: "doc-b" });

  expect(bytesToHex(a.replicaPk)).not.toBe(bytesToHex(b.replicaPk));
  expect(bytesToHex(a.issuerPk)).not.toBe(bytesToHex(b.issuerPk));

  // Sanity: both bundles should still decrypt under the same device wrap key.
  const sealedA = await sealTreecrdtDocKeyBundleV1({ wrapKey, bundle: a });
  const sealedB = await sealTreecrdtDocKeyBundleV1({ wrapKey, bundle: b });
  const openedA = await openTreecrdtDocKeyBundleV1({ wrapKey, docId: "doc-a", sealed: sealedA });
  const openedB = await openTreecrdtDocKeyBundleV1({ wrapKey, docId: "doc-b", sealed: sealedB });
  expect(bytesToHex(openedA.replicaPk)).toBe(bytesToHex(a.replicaPk));
  expect(bytesToHex(openedB.replicaPk)).toBe(bytesToHex(b.replicaPk));
});

test("keystore v1: seal/open issuer key roundtrip", async () => {
  const docId = "doc-issuer-1";
  const wrapKey = generateTreecrdtDeviceWrapKeyV1();
  const issuer = await generateTreecrdtIssuerKeyV1({ docId });

  const sealed = await sealTreecrdtIssuerKeyV1({ wrapKey, docId, issuerSk: issuer.issuerSk });
  const opened = await openTreecrdtIssuerKeyV1({ wrapKey, docId, sealed });

  expect(opened.docId).toBe(docId);
  expect(bytesToHex(opened.issuerSk)).toBe(bytesToHex(issuer.issuerSk));
  expect(bytesToHex(opened.issuerPk)).toBe(bytesToHex(issuer.issuerPk));
});

test("keystore v1: open issuer key fails with wrong docId (AAD mismatch)", async () => {
  const wrapKey = generateTreecrdtDeviceWrapKeyV1();
  const issuer = await generateTreecrdtIssuerKeyV1({ docId: "doc-a" });
  const sealed = await sealTreecrdtIssuerKeyV1({ wrapKey, docId: "doc-a", issuerSk: issuer.issuerSk });

  await expect(openTreecrdtIssuerKeyV1({ wrapKey, docId: "doc-b", sealed })).rejects.toThrow();
});

test("keystore v1: seal/open local identity roundtrip", async () => {
  const docId = "doc-local-1";
  const replicaLabel = "replica-a";
  const wrapKey = generateTreecrdtDeviceWrapKeyV1();
  const identity = await generateTreecrdtLocalIdentityV1({ docId, replicaLabel, localTokens: [new Uint8Array([1, 2, 3])] });

  const sealed = await sealTreecrdtLocalIdentityV1({
    wrapKey,
    docId,
    replicaLabel,
    localSk: identity.localSk,
    localTokens: identity.localTokens,
  });
  const opened = await openTreecrdtLocalIdentityV1({ wrapKey, docId, replicaLabel, sealed });

  expect(opened.docId).toBe(docId);
  expect(opened.replicaLabel).toBe(replicaLabel);
  expect(bytesToHex(opened.localSk)).toBe(bytesToHex(identity.localSk));
  expect(bytesToHex(opened.localPk)).toBe(bytesToHex(identity.localPk));
  expect(opened.localTokens.length).toBe(1);
  expect(bytesToHex(opened.localTokens[0]!)).toBe(bytesToHex(identity.localTokens[0]!));
});

test("keystore v1: open local identity fails with wrong replicaLabel (AAD mismatch)", async () => {
  const wrapKey = generateTreecrdtDeviceWrapKeyV1();
  const identity = await generateTreecrdtLocalIdentityV1({ docId: "doc-a", replicaLabel: "replica-a" });
  const sealed = await sealTreecrdtLocalIdentityV1({
    wrapKey,
    docId: "doc-a",
    replicaLabel: "replica-a",
    localSk: identity.localSk,
    localTokens: [],
  });

  await expect(openTreecrdtLocalIdentityV1({ wrapKey, docId: "doc-a", replicaLabel: "replica-b", sealed })).rejects.toThrow();
});

