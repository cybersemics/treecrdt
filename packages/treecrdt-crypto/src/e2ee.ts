import { utf8ToBytes } from "@noble/hashes/utils";

import {
  aesGcmDecrypt,
  aesGcmEncrypt,
  assertBytes,
  assertLen,
  assertMap,
  assertString,
  concatBytes,
  decodeCbor,
  encodeCbor,
  mapGet,
  randomBytes,
} from "./internal/util.js";

const DOC_PAYLOAD_KEY_LEN = 32;
const AES_GCM_NONCE_LEN = 12;

const ENCRYPTED_PAYLOAD_V1_TAG = "treecrdt/payload-encrypted/v1";
const ENCRYPTED_PAYLOAD_V1_AAD_DOMAIN = utf8ToBytes("treecrdt/payload-encrypted/v1");

const PAYLOAD_KEY_ID_MAX_LEN = 128;
const PAYLOAD_KEY_ID_PATTERN = /^[A-Za-z0-9._:-]+$/;

function payloadAadV1(docId: string): Uint8Array {
  return concatBytes(ENCRYPTED_PAYLOAD_V1_AAD_DOMAIN, utf8ToBytes(docId));
}

function bytesToHex(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}

function randomPayloadKeyIdV1(): string {
  return `k${bytesToHex(randomBytes(8))}`;
}

function assertPayloadKeyIdV1(kid: string, field: string): string {
  const clean = kid.trim();
  if (clean.length === 0) throw new Error(`${field} must not be empty`);
  if (clean.length > PAYLOAD_KEY_ID_MAX_LEN) {
    throw new Error(`${field} too long (max ${PAYLOAD_KEY_ID_MAX_LEN})`);
  }
  if (!PAYLOAD_KEY_ID_PATTERN.test(clean)) {
    throw new Error(`${field} contains unsupported characters`);
  }
  return clean;
}

export type TreecrdtPayloadKeyringV1 = {
  activeKid: string;
  keys: Record<string, Uint8Array>;
};

function clonePayloadKeyringV1(keyring: TreecrdtPayloadKeyringV1): TreecrdtPayloadKeyringV1 {
  if (!keyring || typeof keyring !== "object") throw new Error("keyring must be an object");

  const activeKid = assertPayloadKeyIdV1(keyring.activeKid, "keyring.activeKid");
  const entries = Object.entries(keyring.keys ?? {});
  if (entries.length === 0) throw new Error("keyring.keys must not be empty");

  const keys: Record<string, Uint8Array> = {};
  for (const [rawKid, rawKey] of entries) {
    const kid = assertPayloadKeyIdV1(rawKid, "keyring.keys[<kid>]");
    assertLen(rawKey, DOC_PAYLOAD_KEY_LEN, `keyring.keys[${kid}]`);
    keys[kid] = new Uint8Array(rawKey);
  }

  if (!Object.prototype.hasOwnProperty.call(keys, activeKid)) {
    throw new Error("keyring.activeKid does not exist in keyring.keys");
  }

  return { activeKid, keys };
}

function decodeEncryptedPayloadV1Strict(bytes: Uint8Array): { nonce: Uint8Array; ct: Uint8Array; kid: string | null } {
  const decoded = decodeCbor(bytes);
  const map = assertMap(decoded, "EncryptedPayloadV1");

  const v = mapGet(map, "v");
  if (v !== 1) throw new Error("EncryptedPayloadV1.v must be 1");

  const t = assertString(mapGet(map, "t"), "EncryptedPayloadV1.t");
  if (t !== ENCRYPTED_PAYLOAD_V1_TAG) throw new Error("EncryptedPayloadV1.t mismatch");

  const alg = assertString(mapGet(map, "alg"), "EncryptedPayloadV1.alg");
  if (alg !== "A256GCM") throw new Error("EncryptedPayloadV1.alg unsupported");

  const nonce = assertLen(assertBytes(mapGet(map, "nonce"), "EncryptedPayloadV1.nonce"), AES_GCM_NONCE_LEN, "nonce");
  const ct = assertBytes(mapGet(map, "ct"), "EncryptedPayloadV1.ct");

  const rawKid = mapGet(map, "kid");
  let kid: string | null = null;
  if (rawKid !== undefined) {
    kid = assertPayloadKeyIdV1(assertString(rawKid, "EncryptedPayloadV1.kid"), "EncryptedPayloadV1.kid");
  }

  return { nonce, ct, kid };
}

function tryDecodeEncryptedPayloadV1(bytes: Uint8Array): { nonce: Uint8Array; ct: Uint8Array; kid: string | null } | null {
  try {
    return decodeEncryptedPayloadV1Strict(bytes);
  } catch {
    return null;
  }
}

export function createTreecrdtPayloadKeyringV1(opts: {
  payloadKey: Uint8Array;
  activeKid?: string;
}): TreecrdtPayloadKeyringV1 {
  assertLen(opts.payloadKey, DOC_PAYLOAD_KEY_LEN, "payloadKey");
  const activeKid = assertPayloadKeyIdV1(opts.activeKid ?? randomPayloadKeyIdV1(), "activeKid");
  return {
    activeKid,
    keys: {
      [activeKid]: new Uint8Array(opts.payloadKey),
    },
  };
}

export function upsertTreecrdtPayloadKeyringKeyV1(opts: {
  keyring: TreecrdtPayloadKeyringV1;
  kid: string;
  payloadKey: Uint8Array;
  makeActive?: boolean;
}): TreecrdtPayloadKeyringV1 {
  const keyring = clonePayloadKeyringV1(opts.keyring);
  const kid = assertPayloadKeyIdV1(opts.kid, "kid");
  assertLen(opts.payloadKey, DOC_PAYLOAD_KEY_LEN, "payloadKey");

  keyring.keys[kid] = new Uint8Array(opts.payloadKey);
  if (opts.makeActive ?? false) keyring.activeKid = kid;

  return keyring;
}

export function rotateTreecrdtPayloadKeyringV1(opts: {
  keyring: TreecrdtPayloadKeyringV1;
  nextKid?: string;
  nextPayloadKey?: Uint8Array;
}): {
  keyring: TreecrdtPayloadKeyringV1;
  rotatedKid: string;
  rotatedPayloadKey: Uint8Array;
} {
  const rotatedKid = assertPayloadKeyIdV1(opts.nextKid ?? randomPayloadKeyIdV1(), "nextKid");
  const rotatedPayloadKey = opts.nextPayloadKey ? new Uint8Array(opts.nextPayloadKey) : randomBytes(DOC_PAYLOAD_KEY_LEN);
  assertLen(rotatedPayloadKey, DOC_PAYLOAD_KEY_LEN, "nextPayloadKey");

  const keyring = upsertTreecrdtPayloadKeyringKeyV1({
    keyring: opts.keyring,
    kid: rotatedKid,
    payloadKey: rotatedPayloadKey,
    makeActive: true,
  });

  return { keyring, rotatedKid, rotatedPayloadKey };
}

export function isTreecrdtEncryptedPayloadV1(bytes: Uint8Array): boolean {
  return tryDecodeEncryptedPayloadV1(bytes) !== null;
}

export function getTreecrdtEncryptedPayloadKeyIdV1(bytes: Uint8Array): string | null {
  const decoded = tryDecodeEncryptedPayloadV1(bytes);
  if (!decoded) return null;
  return decoded.kid;
}

export async function encryptTreecrdtPayloadV1(opts: {
  docId: string;
  payloadKey: Uint8Array;
  plaintext: Uint8Array;
  keyId?: string;
}): Promise<Uint8Array> {
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error("docId must not be empty");
  assertLen(opts.payloadKey, DOC_PAYLOAD_KEY_LEN, "payloadKey");

  const keyId = assertPayloadKeyIdV1(opts.keyId ?? "k0", "keyId");
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
  envelope.set("kid", keyId);
  return encodeCbor(envelope);
}

export async function encryptTreecrdtPayloadWithKeyringV1(opts: {
  docId: string;
  keyring: TreecrdtPayloadKeyringV1;
  plaintext: Uint8Array;
}): Promise<Uint8Array> {
  const keyring = clonePayloadKeyringV1(opts.keyring);
  const payloadKey = keyring.keys[keyring.activeKid];
  if (!payloadKey) throw new Error("active payload key is missing");

  return await encryptTreecrdtPayloadV1({
    docId: opts.docId,
    payloadKey,
    plaintext: opts.plaintext,
    keyId: keyring.activeKid,
  });
}

export async function decryptTreecrdtPayloadV1(opts: {
  docId: string;
  payloadKey: Uint8Array;
  ciphertext: Uint8Array;
}): Promise<Uint8Array> {
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error("docId must not be empty");
  assertLen(opts.payloadKey, DOC_PAYLOAD_KEY_LEN, "payloadKey");

  const decoded = decodeEncryptedPayloadV1Strict(opts.ciphertext);
  return await aesGcmDecrypt({
    key: opts.payloadKey,
    nonce: decoded.nonce,
    ciphertext: decoded.ct,
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

export type TreecrdtMaybeDecryptWithKeyringV1Result =
  | {
      encrypted: false;
      keyMissing: false;
      keyId: null;
      plaintext: Uint8Array;
    }
  | {
      encrypted: true;
      keyMissing: false;
      keyId: string | null;
      plaintext: Uint8Array;
    }
  | {
      encrypted: true;
      keyMissing: true;
      keyId: string | null;
      plaintext: null;
    };

export async function maybeDecryptTreecrdtPayloadWithKeyringV1(opts: {
  docId: string;
  keyring: TreecrdtPayloadKeyringV1;
  bytes: Uint8Array;
}): Promise<TreecrdtMaybeDecryptWithKeyringV1Result> {
  const parsed = tryDecodeEncryptedPayloadV1(opts.bytes);
  if (!parsed) {
    return {
      encrypted: false,
      keyMissing: false,
      keyId: null,
      plaintext: opts.bytes,
    };
  }

  const keyring = clonePayloadKeyringV1(opts.keyring);

  if (parsed.kid === null) {
    return {
      encrypted: true,
      keyMissing: true,
      keyId: null,
      plaintext: null,
    };
  }

  const payloadKey = keyring.keys[parsed.kid];
  if (!payloadKey) {
    return {
      encrypted: true,
      keyMissing: true,
      keyId: parsed.kid,
      plaintext: null,
    };
  }

  const plaintext = await aesGcmDecrypt({
    key: payloadKey,
    nonce: parsed.nonce,
    ciphertext: parsed.ct,
    aad: payloadAadV1(opts.docId),
  });

  return {
    encrypted: true,
    keyMissing: false,
    keyId: parsed.kid,
    plaintext,
  };
}
