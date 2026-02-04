import { decode as cborDecode, encode as cborEncode, rfc8949EncodeOptions } from "cborg";

import { utf8ToBytes } from "@noble/hashes/utils";

const DOC_PAYLOAD_KEY_LEN = 32;
const AES_GCM_NONCE_LEN = 12;

const ENCRYPTED_PAYLOAD_V1_TAG = "treecrdt/payload-encrypted/v1";
const ENCRYPTED_PAYLOAD_V1_AAD_DOMAIN = utf8ToBytes("treecrdt/payload-encrypted/v1");

function encodeCbor(value: unknown): Uint8Array {
  return cborEncode(value, rfc8949EncodeOptions);
}

function decodeCbor(bytes: Uint8Array): unknown {
  return cborDecode(bytes, { useMaps: true });
}

function concatBytes(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

function assertBytes(val: unknown, field: string): Uint8Array {
  if (!(val instanceof Uint8Array)) throw new Error(`${field} must be bytes`);
  return val;
}

function assertString(val: unknown, field: string): string {
  if (typeof val !== "string") throw new Error(`${field} must be a string`);
  return val;
}

function assertLen(bytes: Uint8Array, expected: number, field: string): Uint8Array {
  if (bytes.length !== expected) {
    throw new Error(`${field}: expected ${expected} bytes, got ${bytes.length}`);
  }
  return bytes;
}

function assertMap(val: unknown, ctx: string): Map<unknown, unknown> {
  if (!(val instanceof Map)) throw new Error(`${ctx} must be a CBOR map`);
  return val;
}

function mapGet(map: Map<unknown, unknown>, key: unknown): unknown {
  return map.has(key) ? map.get(key) : undefined;
}

function randomBytes(len: number): Uint8Array {
  const cryptoObj = (globalThis as any).crypto as { getRandomValues?: (arr: Uint8Array) => Uint8Array } | undefined;
  if (!cryptoObj?.getRandomValues) throw new Error("crypto.getRandomValues is not available");
  const bytes = new Uint8Array(len);
  cryptoObj.getRandomValues(bytes);
  return bytes;
}

async function aesGcmEncrypt(opts: {
  key: Uint8Array;
  nonce: Uint8Array;
  plaintext: Uint8Array;
  aad?: Uint8Array;
}): Promise<Uint8Array> {
  const cryptoObj = (globalThis as any).crypto as { subtle?: any } | undefined;
  if (!cryptoObj?.subtle) throw new Error("crypto.subtle is not available");

  const key = await cryptoObj.subtle.importKey("raw", opts.key, { name: "AES-GCM" }, false, ["encrypt"]);
  const ciphertext = await cryptoObj.subtle.encrypt(
    {
      name: "AES-GCM",
      iv: opts.nonce,
      ...(opts.aad ? { additionalData: opts.aad } : {}),
      tagLength: 128,
    },
    key,
    opts.plaintext
  );
  return new Uint8Array(ciphertext);
}

async function aesGcmDecrypt(opts: {
  key: Uint8Array;
  nonce: Uint8Array;
  ciphertext: Uint8Array;
  aad?: Uint8Array;
}): Promise<Uint8Array> {
  const cryptoObj = (globalThis as any).crypto as { subtle?: any } | undefined;
  if (!cryptoObj?.subtle) throw new Error("crypto.subtle is not available");

  const key = await cryptoObj.subtle.importKey("raw", opts.key, { name: "AES-GCM" }, false, ["decrypt"]);
  const plaintext = await cryptoObj.subtle.decrypt(
    {
      name: "AES-GCM",
      iv: opts.nonce,
      ...(opts.aad ? { additionalData: opts.aad } : {}),
      tagLength: 128,
    },
    key,
    opts.ciphertext
  );
  return new Uint8Array(plaintext);
}

function payloadAadV1(docId: string): Uint8Array {
  return concatBytes(ENCRYPTED_PAYLOAD_V1_AAD_DOMAIN, utf8ToBytes(docId));
}

function tryDecodeEncryptedPayloadV1(bytes: Uint8Array): { nonce: Uint8Array; ct: Uint8Array } | null {
  let decoded: unknown;
  try {
    decoded = decodeCbor(bytes);
  } catch {
    return null;
  }
  if (!(decoded instanceof Map)) return null;

  const map = decoded as Map<unknown, unknown>;
  const v = mapGet(map, "v");
  if (v !== 1) return null;
  const t = mapGet(map, "t");
  if (t !== ENCRYPTED_PAYLOAD_V1_TAG) return null;

  const alg = mapGet(map, "alg");
  if (alg !== "A256GCM") return null;

  const nonce = mapGet(map, "nonce");
  if (!(nonce instanceof Uint8Array)) return null;
  if (nonce.length !== AES_GCM_NONCE_LEN) return null;

  const ct = mapGet(map, "ct");
  if (!(ct instanceof Uint8Array)) return null;

  return { nonce, ct };
}

export function isTreecrdtEncryptedPayloadV1(bytes: Uint8Array): boolean {
  return tryDecodeEncryptedPayloadV1(bytes) !== null;
}

export async function encryptTreecrdtPayloadV1(opts: {
  docId: string;
  payloadKey: Uint8Array;
  plaintext: Uint8Array;
}): Promise<Uint8Array> {
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error("docId must not be empty");
  assertLen(opts.payloadKey, DOC_PAYLOAD_KEY_LEN, "payloadKey");

  const nonce = randomBytes(AES_GCM_NONCE_LEN);
  const ciphertext = await aesGcmEncrypt({
    key: opts.payloadKey,
    nonce,
    plaintext: opts.plaintext,
    aad: payloadAadV1(opts.docId),
  });

  const envelope = new Map<unknown, unknown>();
  envelope.set("v", 1);
  envelope.set("t", ENCRYPTED_PAYLOAD_V1_TAG);
  envelope.set("alg", "A256GCM");
  envelope.set("nonce", nonce);
  envelope.set("ct", ciphertext);
  return encodeCbor(envelope);
}

export async function decryptTreecrdtPayloadV1(opts: {
  docId: string;
  payloadKey: Uint8Array;
  ciphertext: Uint8Array;
}): Promise<Uint8Array> {
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error("docId must not be empty");
  assertLen(opts.payloadKey, DOC_PAYLOAD_KEY_LEN, "payloadKey");

  const decoded = decodeCbor(opts.ciphertext);
  const map = assertMap(decoded, "EncryptedPayloadV1");

  const v = mapGet(map, "v");
  if (v !== 1) throw new Error("EncryptedPayloadV1.v must be 1");
  const t = assertString(mapGet(map, "t"), "EncryptedPayloadV1.t");
  if (t !== ENCRYPTED_PAYLOAD_V1_TAG) throw new Error("EncryptedPayloadV1.t mismatch");
  const alg = assertString(mapGet(map, "alg"), "EncryptedPayloadV1.alg");
  if (alg !== "A256GCM") throw new Error("EncryptedPayloadV1.alg unsupported");

  const nonce = assertLen(assertBytes(mapGet(map, "nonce"), "EncryptedPayloadV1.nonce"), AES_GCM_NONCE_LEN, "nonce");
  const ct = assertBytes(mapGet(map, "ct"), "EncryptedPayloadV1.ct");

  return await aesGcmDecrypt({
    key: opts.payloadKey,
    nonce,
    ciphertext: ct,
    aad: payloadAadV1(opts.docId),
  });
}

export async function maybeDecryptTreecrdtPayloadV1(opts: {
  docId: string;
  payloadKey: Uint8Array;
  bytes: Uint8Array;
}): Promise<{ plaintext: Uint8Array; encrypted: boolean }> {
  const parsed = tryDecodeEncryptedPayloadV1(opts.bytes);
  if (!parsed) return { plaintext: opts.bytes, encrypted: false };

  const plaintext = await aesGcmDecrypt({
    key: opts.payloadKey,
    nonce: parsed.nonce,
    ciphertext: parsed.ct,
    aad: payloadAadV1(opts.docId),
  });

  return { plaintext, encrypted: true };
}

