import { decode as cborDecode, encode as cborEncode, rfc8949EncodeOptions } from "cborg";

import { coseSign1Ed25519, coseVerifySign1Ed25519 } from "./cose.js";

const ED25519_PUBLIC_KEY_LEN = 32;

export type Ed25519PublicKey = Uint8Array;
export type Ed25519PrivateKey = Uint8Array;

function encodeCbor(value: unknown): Uint8Array {
  return cborEncode(value, rfc8949EncodeOptions);
}

function decodeCbor(bytes: Uint8Array): unknown {
  return cborDecode(bytes, { useMaps: true });
}

function assertBytes(val: unknown, field: string): Uint8Array {
  if (!(val instanceof Uint8Array)) throw new Error(`${field} must be bytes`);
  return val;
}

function assertString(val: unknown, field: string): string {
  if (typeof val !== "string") throw new Error(`${field} must be a string`);
  return val;
}

function assertNumber(val: unknown, field: string): number {
  if (typeof val !== "number" || !Number.isFinite(val)) throw new Error(`${field} must be a finite number`);
  return val;
}

function assertEd25519PublicKey(val: unknown, field: string): Ed25519PublicKey {
  const bytes = assertBytes(val, field);
  if (bytes.length !== ED25519_PUBLIC_KEY_LEN) {
    throw new Error(`${field} must be ${ED25519_PUBLIC_KEY_LEN} bytes (ed25519 pubkey), got ${bytes.length}`);
  }
  return bytes;
}

function assertPayloadMap(payload: unknown, ctx: string): Map<unknown, unknown> {
  if (!(payload instanceof Map)) throw new Error(`${ctx} payload must be a CBOR map`);
  return payload;
}

function get(map: Map<unknown, unknown>, key: string): unknown {
  return map.has(key) ? map.get(key) : undefined;
}

const DEVICE_CERT_V1_TAG = "treecrdt/device-cert/v1";
const REPLICA_CERT_V1_TAG = "treecrdt/replica-cert/v1";

export type DeviceCertV1Claims = {
  devicePublicKey: Ed25519PublicKey;
  iat?: number;
  exp?: number;
};

export type ReplicaCertV1Claims = {
  docId: string;
  replicaPublicKey: Ed25519PublicKey;
  iat?: number;
  exp?: number;
};

export function issueDeviceCertV1(opts: {
  identityPrivateKey: Ed25519PrivateKey;
  devicePublicKey: Ed25519PublicKey;
  iat?: number;
  exp?: number;
}): Uint8Array {
  assertEd25519PublicKey(opts.devicePublicKey, "devicePublicKey");

  const claims = new Map<unknown, unknown>();
  claims.set("v", 1);
  claims.set("t", DEVICE_CERT_V1_TAG);
  claims.set("device_pk", opts.devicePublicKey);
  if (opts.iat !== undefined) claims.set("iat", opts.iat);
  if (opts.exp !== undefined) claims.set("exp", opts.exp);

  return coseSign1Ed25519({ payload: encodeCbor(claims), privateKey: opts.identityPrivateKey });
}

export async function verifyDeviceCertV1(opts: {
  certBytes: Uint8Array;
  identityPublicKey: Ed25519PublicKey;
  nowSec?: () => number;
}): Promise<DeviceCertV1Claims> {
  assertEd25519PublicKey(opts.identityPublicKey, "identityPublicKey");

  const payloadBytes = await coseVerifySign1Ed25519({ bytes: opts.certBytes, publicKey: opts.identityPublicKey });
  const decoded = decodeCbor(payloadBytes);
  const map = assertPayloadMap(decoded, "DeviceCertV1");

  const v = get(map, "v");
  if (v !== 1) throw new Error("DeviceCertV1.v must be 1");
  const t = assertString(get(map, "t"), "DeviceCertV1.t");
  if (t !== DEVICE_CERT_V1_TAG) throw new Error("DeviceCertV1.t mismatch");

  const devicePk = assertEd25519PublicKey(get(map, "device_pk"), "DeviceCertV1.device_pk");

  const iatRaw = get(map, "iat");
  const expRaw = get(map, "exp");
  const iat = iatRaw === undefined ? undefined : assertNumber(iatRaw, "DeviceCertV1.iat");
  const exp = expRaw === undefined ? undefined : assertNumber(expRaw, "DeviceCertV1.exp");

  if (opts.nowSec && exp !== undefined) {
    const now = opts.nowSec();
    if (now > exp) throw new Error("DeviceCertV1 expired");
  }

  return { devicePublicKey: devicePk, ...(iat !== undefined ? { iat } : {}), ...(exp !== undefined ? { exp } : {}) };
}

export function issueReplicaCertV1(opts: {
  devicePrivateKey: Ed25519PrivateKey;
  docId: string;
  replicaPublicKey: Ed25519PublicKey;
  iat?: number;
  exp?: number;
}): Uint8Array {
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error("docId must not be empty");
  assertEd25519PublicKey(opts.replicaPublicKey, "replicaPublicKey");

  const claims = new Map<unknown, unknown>();
  claims.set("v", 1);
  claims.set("t", REPLICA_CERT_V1_TAG);
  claims.set("doc_id", opts.docId);
  claims.set("replica_pk", opts.replicaPublicKey);
  if (opts.iat !== undefined) claims.set("iat", opts.iat);
  if (opts.exp !== undefined) claims.set("exp", opts.exp);

  return coseSign1Ed25519({ payload: encodeCbor(claims), privateKey: opts.devicePrivateKey });
}

export async function verifyReplicaCertV1(opts: {
  certBytes: Uint8Array;
  devicePublicKey: Ed25519PublicKey;
  expectedDocId?: string;
  nowSec?: () => number;
}): Promise<ReplicaCertV1Claims> {
  assertEd25519PublicKey(opts.devicePublicKey, "devicePublicKey");

  const payloadBytes = await coseVerifySign1Ed25519({ bytes: opts.certBytes, publicKey: opts.devicePublicKey });
  const decoded = decodeCbor(payloadBytes);
  const map = assertPayloadMap(decoded, "ReplicaCertV1");

  const v = get(map, "v");
  if (v !== 1) throw new Error("ReplicaCertV1.v must be 1");
  const t = assertString(get(map, "t"), "ReplicaCertV1.t");
  if (t !== REPLICA_CERT_V1_TAG) throw new Error("ReplicaCertV1.t mismatch");

  const docId = assertString(get(map, "doc_id"), "ReplicaCertV1.doc_id");
  if (opts.expectedDocId !== undefined && docId !== opts.expectedDocId) {
    throw new Error("ReplicaCertV1.doc_id mismatch");
  }

  const replicaPk = assertEd25519PublicKey(get(map, "replica_pk"), "ReplicaCertV1.replica_pk");

  const iatRaw = get(map, "iat");
  const expRaw = get(map, "exp");
  const iat = iatRaw === undefined ? undefined : assertNumber(iatRaw, "ReplicaCertV1.iat");
  const exp = expRaw === undefined ? undefined : assertNumber(expRaw, "ReplicaCertV1.exp");

  if (opts.nowSec && exp !== undefined) {
    const now = opts.nowSec();
    if (now > exp) throw new Error("ReplicaCertV1 expired");
  }

  return {
    docId,
    replicaPublicKey: replicaPk,
    ...(iat !== undefined ? { iat } : {}),
    ...(exp !== undefined ? { exp } : {}),
  };
}

export async function verifyReplicaChainV1(opts: {
  identityPublicKey: Ed25519PublicKey;
  deviceCertBytes: Uint8Array;
  replicaCertBytes: Uint8Array;
  expectedDocId?: string;
  expectedReplicaPublicKey?: Ed25519PublicKey;
  nowSec?: () => number;
}): Promise<{ devicePublicKey: Ed25519PublicKey; replicaPublicKey: Ed25519PublicKey }> {
  const device = await verifyDeviceCertV1({
    certBytes: opts.deviceCertBytes,
    identityPublicKey: opts.identityPublicKey,
    nowSec: opts.nowSec,
  });
  const replica = await verifyReplicaCertV1({
    certBytes: opts.replicaCertBytes,
    devicePublicKey: device.devicePublicKey,
    expectedDocId: opts.expectedDocId,
    nowSec: opts.nowSec,
  });

  if (opts.expectedReplicaPublicKey) {
    assertEd25519PublicKey(opts.expectedReplicaPublicKey, "expectedReplicaPublicKey");
    const a = opts.expectedReplicaPublicKey;
    const b = replica.replicaPublicKey;
    if (a.length !== b.length) throw new Error("ReplicaCertV1.replica_pk mismatch");
    for (let i = 0; i < a.length; i += 1) {
      if (a[i] !== b[i]) throw new Error("ReplicaCertV1.replica_pk mismatch");
    }
  }

  return { devicePublicKey: device.devicePublicKey, replicaPublicKey: replica.replicaPublicKey };
}

