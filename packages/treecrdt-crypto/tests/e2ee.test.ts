import { expect, test } from "vitest";

import { generateTreecrdtDocPayloadKeyV1 } from "../dist/keystore.js";
import { encryptTreecrdtPayloadV1, maybeDecryptTreecrdtPayloadV1 } from "../dist/e2ee.js";

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

