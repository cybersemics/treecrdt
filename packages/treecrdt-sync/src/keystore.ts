import { encode as cborEncode, decode as cborDecode, rfc8949EncodeOptions } from "cborg";

import { utf8ToBytes } from "@noble/hashes/utils";
import { sha512 } from "@noble/hashes/sha512";
import { hashes as ed25519Hashes, getPublicKey, utils as ed25519Utils } from "@noble/ed25519";

const DEVICE_WRAP_KEY_LEN = 32;
const AES_GCM_NONCE_LEN = 12;
const ED25519_SECRET_KEY_LEN = 32;

const DOC_KEY_BUNDLE_V1_TAG = "treecrdt/doc-key-bundle/v1";
const SEALED_DOC_KEY_BUNDLE_V1_TAG = "treecrdt/doc-key-bundle-sealed/v1";
const SEALED_DOC_KEY_BUNDLE_V1_AAD_DOMAIN = utf8ToBytes("treecrdt/doc-key-bundle-sealed/v1");

let ed25519Ready = false;
function ensureEd25519(): void {
  if (ed25519Ready) return;
  ed25519Hashes.sha512 = sha512;
  ed25519Ready = true;
}

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

function get(map: Map<unknown, unknown>, key: string): unknown {
  return mapGet(map, key);
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

export type TreecrdtDeviceWrapKeyV1 = Uint8Array;

export type TreecrdtDocKeyBundleV1 = {
  docId: string;
  issuerSk: Uint8Array;
  issuerPk: Uint8Array;
  replicaSk: Uint8Array;
  replicaPk: Uint8Array;
};

export type TreecrdtSealedDocKeyBundleV1 = Uint8Array;

export function generateTreecrdtDeviceWrapKeyV1(): TreecrdtDeviceWrapKeyV1 {
  return randomBytes(DEVICE_WRAP_KEY_LEN);
}

export async function generateTreecrdtDocKeyBundleV1(opts: { docId: string }): Promise<TreecrdtDocKeyBundleV1> {
  ensureEd25519();
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error("docId must not be empty");

  const issuerSk = ed25519Utils.randomSecretKey();
  const issuerPk = await getPublicKey(issuerSk);
  const replicaSk = ed25519Utils.randomSecretKey();
  const replicaPk = await getPublicKey(replicaSk);

  return {
    docId: opts.docId,
    issuerSk,
    issuerPk,
    replicaSk,
    replicaPk,
  };
}

function encodeDocKeyBundleV1(bundle: TreecrdtDocKeyBundleV1): Uint8Array {
  assertLen(bundle.issuerSk, ED25519_SECRET_KEY_LEN, "issuerSk");
  assertLen(bundle.replicaSk, ED25519_SECRET_KEY_LEN, "replicaSk");

  const claims = new Map<unknown, unknown>();
  claims.set("v", 1);
  claims.set("t", DOC_KEY_BUNDLE_V1_TAG);
  claims.set("doc_id", bundle.docId);
  claims.set("issuer_sk", bundle.issuerSk);
  claims.set("replica_sk", bundle.replicaSk);
  return encodeCbor(claims);
}

async function decodeDocKeyBundleV1(bytes: Uint8Array): Promise<TreecrdtDocKeyBundleV1> {
  ensureEd25519();
  const decoded = decodeCbor(bytes);
  const map = assertMap(decoded, "DocKeyBundleV1");

  const v = get(map, "v");
  if (v !== 1) throw new Error("DocKeyBundleV1.v must be 1");
  const t = assertString(get(map, "t"), "DocKeyBundleV1.t");
  if (t !== DOC_KEY_BUNDLE_V1_TAG) throw new Error("DocKeyBundleV1.t mismatch");

  const docId = assertString(get(map, "doc_id"), "DocKeyBundleV1.doc_id");
  const issuerSk = assertLen(assertBytes(get(map, "issuer_sk"), "DocKeyBundleV1.issuer_sk"), ED25519_SECRET_KEY_LEN, "issuer_sk");
  const replicaSk = assertLen(
    assertBytes(get(map, "replica_sk"), "DocKeyBundleV1.replica_sk"),
    ED25519_SECRET_KEY_LEN,
    "replica_sk"
  );

  const issuerPk = await getPublicKey(issuerSk);
  const replicaPk = await getPublicKey(replicaSk);

  return { docId, issuerSk, issuerPk, replicaSk, replicaPk };
}

function sealedAadV1(docId: string): Uint8Array {
  return concatBytes(SEALED_DOC_KEY_BUNDLE_V1_AAD_DOMAIN, utf8ToBytes(docId));
}

export async function sealTreecrdtDocKeyBundleV1(opts: {
  wrapKey: TreecrdtDeviceWrapKeyV1;
  bundle: TreecrdtDocKeyBundleV1;
}): Promise<TreecrdtSealedDocKeyBundleV1> {
  assertLen(opts.wrapKey, DEVICE_WRAP_KEY_LEN, "wrapKey");
  if (!opts.bundle.docId || opts.bundle.docId.trim().length === 0) throw new Error("bundle.docId must not be empty");

  const nonce = randomBytes(AES_GCM_NONCE_LEN);
  const plaintext = encodeDocKeyBundleV1(opts.bundle);
  const ciphertext = await aesGcmEncrypt({ key: opts.wrapKey, nonce, plaintext, aad: sealedAadV1(opts.bundle.docId) });

  const envelope = new Map<unknown, unknown>();
  envelope.set("v", 1);
  envelope.set("t", SEALED_DOC_KEY_BUNDLE_V1_TAG);
  envelope.set("alg", "A256GCM");
  envelope.set("nonce", nonce);
  envelope.set("ct", ciphertext);
  return encodeCbor(envelope);
}

export async function openTreecrdtDocKeyBundleV1(opts: {
  wrapKey: TreecrdtDeviceWrapKeyV1;
  docId: string;
  sealed: TreecrdtSealedDocKeyBundleV1;
}): Promise<TreecrdtDocKeyBundleV1> {
  assertLen(opts.wrapKey, DEVICE_WRAP_KEY_LEN, "wrapKey");
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error("docId must not be empty");

  const decoded = decodeCbor(opts.sealed);
  const map = assertMap(decoded, "SealedDocKeyBundleV1");

  const v = get(map, "v");
  if (v !== 1) throw new Error("SealedDocKeyBundleV1.v must be 1");
  const t = assertString(get(map, "t"), "SealedDocKeyBundleV1.t");
  if (t !== SEALED_DOC_KEY_BUNDLE_V1_TAG) throw new Error("SealedDocKeyBundleV1.t mismatch");
  const alg = assertString(get(map, "alg"), "SealedDocKeyBundleV1.alg");
  if (alg !== "A256GCM") throw new Error("SealedDocKeyBundleV1.alg unsupported");

  const nonce = assertLen(assertBytes(get(map, "nonce"), "SealedDocKeyBundleV1.nonce"), AES_GCM_NONCE_LEN, "nonce");
  const ct = assertBytes(get(map, "ct"), "SealedDocKeyBundleV1.ct");

  const plaintext = await aesGcmDecrypt({ key: opts.wrapKey, nonce, ciphertext: ct, aad: sealedAadV1(opts.docId) });
  const bundle = await decodeDocKeyBundleV1(plaintext);

  if (bundle.docId !== opts.docId) throw new Error("DocKeyBundleV1.doc_id mismatch");

  return bundle;
}
