import { expect, test } from "vitest";

import { generateTreecrdtDocPayloadKeyV1 } from "../dist/keystore.js";
import {
  createTreecrdtPayloadKeyringV1,
  encryptTreecrdtPayloadV1,
  encryptTreecrdtPayloadWithKeyringV1,
  getTreecrdtEncryptedPayloadKeyIdV1,
  maybeDecryptTreecrdtPayloadV1,
  maybeDecryptTreecrdtPayloadWithKeyringV1,
  rotateTreecrdtPayloadKeyringV1,
} from "../dist/e2ee.js";

function bytesToHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("hex");
}

test("e2ee v1: encrypt/decrypt payload roundtrip", async () => {
  const docId = "doc-e2ee-1";
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const plaintext = new TextEncoder().encode("hello");

  const encrypted = await encryptTreecrdtPayloadV1({ docId, payloadKey, plaintext });
  const res = await maybeDecryptTreecrdtPayloadV1({ docId, payloadKey, bytes: encrypted });

  expect(res.encrypted).toBe(true);
  expect(new TextDecoder().decode(res.plaintext)).toBe("hello");
});

test("e2ee v1: decrypt fails with wrong docId (AAD mismatch)", async () => {
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId: "doc-a" });
  const plaintext = new TextEncoder().encode("hello");
  const encrypted = await encryptTreecrdtPayloadV1({ docId: "doc-a", payloadKey, plaintext });

  await expect(maybeDecryptTreecrdtPayloadV1({ docId: "doc-b", payloadKey, bytes: encrypted })).rejects.toThrow();
});

test("e2ee v1: maybeDecrypt returns bytes unchanged if not encrypted", async () => {
  const docId = "doc-e2ee-2";
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const bytes = new Uint8Array([1, 2, 3, 4]);

  const res = await maybeDecryptTreecrdtPayloadV1({ docId, payloadKey, bytes });

  expect(res.encrypted).toBe(false);
  expect(bytesToHex(res.plaintext)).toBe(bytesToHex(bytes));
});

test("e2ee v1 keyring: encrypt tags ciphertext with active key id", async () => {
  const docId = "doc-e2ee-kid";
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const keyring = createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: "epoch-1" });

  const plaintext = new TextEncoder().encode("payload with kid");
  const encrypted = await encryptTreecrdtPayloadWithKeyringV1({ docId, keyring, plaintext });

  expect(getTreecrdtEncryptedPayloadKeyIdV1(encrypted)).toBe("epoch-1");

  const decrypted = await maybeDecryptTreecrdtPayloadWithKeyringV1({ docId, keyring, bytes: encrypted });
  expect(decrypted.encrypted).toBe(true);
  expect(decrypted.keyMissing).toBe(false);
  expect(decrypted.keyId).toBe("epoch-1");
  if (decrypted.plaintext === null) throw new Error("plaintext is null");
  expect(new TextDecoder().decode(decrypted.plaintext)).toBe("payload with kid");
});

test("e2ee v1 keyring: keyMissing=true when ciphertext key id is unavailable", async () => {
  const docId = "doc-e2ee-missing-key";
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const sender = createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: "epoch-1" });

  const encrypted = await encryptTreecrdtPayloadWithKeyringV1({
    docId,
    keyring: sender,
    plaintext: new TextEncoder().encode("secret"),
  });

  const { payloadKey: otherKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const receiver = createTreecrdtPayloadKeyringV1({ payloadKey: otherKey, activeKid: "epoch-2" });

  const decrypted = await maybeDecryptTreecrdtPayloadWithKeyringV1({ docId, keyring: receiver, bytes: encrypted });
  expect(decrypted.encrypted).toBe(true);
  expect(decrypted.keyMissing).toBe(true);
  expect(decrypted.keyId).toBe("epoch-1");
  expect(decrypted.plaintext).toBeNull();
});

test("e2ee v1 keyring: rotated keyring decrypts both old and new payloads", async () => {
  const docId = "doc-e2ee-rotate";
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });

  const keyringV1 = createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: "epoch-1" });
  const oldEncrypted = await encryptTreecrdtPayloadWithKeyringV1({
    docId,
    keyring: keyringV1,
    plaintext: new TextEncoder().encode("before rotate"),
  });

  const { keyring: keyringV2 } = rotateTreecrdtPayloadKeyringV1({ keyring: keyringV1, nextKid: "epoch-2" });
  const newEncrypted = await encryptTreecrdtPayloadWithKeyringV1({
    docId,
    keyring: keyringV2,
    plaintext: new TextEncoder().encode("after rotate"),
  });

  const oldDecrypted = await maybeDecryptTreecrdtPayloadWithKeyringV1({ docId, keyring: keyringV2, bytes: oldEncrypted });
  const newDecrypted = await maybeDecryptTreecrdtPayloadWithKeyringV1({ docId, keyring: keyringV2, bytes: newEncrypted });

  expect(oldDecrypted.keyMissing).toBe(false);
  expect(oldDecrypted.keyId).toBe("epoch-1");
  if (oldDecrypted.plaintext === null) throw new Error("old plaintext is null");
  expect(new TextDecoder().decode(oldDecrypted.plaintext)).toBe("before rotate");

  expect(newDecrypted.keyMissing).toBe(false);
  expect(newDecrypted.keyId).toBe("epoch-2");
  if (newDecrypted.plaintext === null) throw new Error("new plaintext is null");
  expect(new TextDecoder().decode(newDecrypted.plaintext)).toBe("after rotate");
});

test("e2ee v1 keyring: decrypt requires matching key id", async () => {
  const docId = "doc-e2ee-kid-required";
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const keyring = createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: "epoch-legacy" });

  const encrypted = await encryptTreecrdtPayloadV1({
    docId,
    payloadKey,
    plaintext: new TextEncoder().encode("legacy format"),
  });

  expect(getTreecrdtEncryptedPayloadKeyIdV1(encrypted)).toBe("k0");

  const decrypted = await maybeDecryptTreecrdtPayloadWithKeyringV1({ docId, keyring, bytes: encrypted });
  expect(decrypted.encrypted).toBe(true);
  expect(decrypted.keyMissing).toBe(true);
  expect(decrypted.keyId).toBe("k0");
  expect(decrypted.plaintext).toBeNull();
});
