import { expect, test } from "vitest";

import { bytesToHex } from "@treecrdt/interface/ids";

import {
  generateTreecrdtDeviceWrapKeyV1,
  generateTreecrdtDocKeyBundleV1,
  openTreecrdtDocKeyBundleV1,
  sealTreecrdtDocKeyBundleV1,
} from "../dist/keystore.js";

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

