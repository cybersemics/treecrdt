import { decode as cborDecode, encode as cborEncode, rfc8949EncodeOptions } from "cborg";

import type { Capability } from "@treecrdt/sync";

import { base64urlDecode, base64urlEncode } from "./base64url.js";
import { coseSign1Ed25519, coseVerifySign1Ed25519 } from "./cose.js";

const TREECRDT_REVOCATION_RECORD_V1_TAG = "treecrdt/revocation/v1";
const REVOCATION_TOKEN_ID_LEN = 16;
const ED25519_PUBLIC_KEY_LEN = 32;

export const TREECRDT_REVOCATION_CAPABILITY = "auth.revocation";

export type TreecrdtRevocationModeV1 = "hard" | "write_cutover";

export type TreecrdtRevocationRecordV1 = {
  docId: string;
  tokenId: Uint8Array;
  mode: TreecrdtRevocationModeV1;
  revSeq: number;
  iat?: number;
  effectiveFromCounter?: number;
  effectiveFromReplica?: Uint8Array;
  effectiveFromLamport?: number;
};

export type VerifiedTreecrdtRevocationRecordV1 = TreecrdtRevocationRecordV1 & {
  issuerPublicKey: Uint8Array;
};

function encodeCbor(value: unknown): Uint8Array {
  return cborEncode(value, rfc8949EncodeOptions);
}

function decodeCbor(bytes: Uint8Array): unknown {
  return cborDecode(bytes, { useMaps: true });
}

function mapGet(map: Map<unknown, unknown>, key: string): unknown {
  return map.has(key) ? map.get(key) : undefined;
}

function assertPayloadMap(payload: unknown, ctx: string): Map<unknown, unknown> {
  if (!(payload instanceof Map)) throw new Error(`${ctx} payload must be a CBOR map`);
  return payload;
}

function assertString(val: unknown, field: string): string {
  if (typeof val !== "string") throw new Error(`${field} must be a string`);
  return val;
}

function assertInteger(val: unknown, field: string): number {
  if (typeof val !== "number" || !Number.isFinite(val) || !Number.isInteger(val) || val < 0) {
    throw new Error(`${field} must be a non-negative integer`);
  }
  return val;
}

function assertBytesLen(val: unknown, expectedLen: number, field: string): Uint8Array {
  if (!(val instanceof Uint8Array)) throw new Error(`${field} must be bytes`);
  if (val.length !== expectedLen) throw new Error(`${field} must be ${expectedLen} bytes`);
  return val;
}

function parseRevocationMode(value: unknown): TreecrdtRevocationModeV1 {
  if (value === "hard" || value === "write_cutover") return value;
  throw new Error("RevocationRecordV1.mode must be \"hard\" or \"write_cutover\"");
}

function validateModeFields(mode: TreecrdtRevocationModeV1, record: {
  effectiveFromCounter?: number;
  effectiveFromReplica?: Uint8Array;
  effectiveFromLamport?: number;
}) {
  if (mode === "hard") return;

  const hasCounter = record.effectiveFromCounter !== undefined;
  const hasLamport = record.effectiveFromLamport !== undefined;
  if (!hasCounter && !hasLamport) {
    throw new Error("RevocationRecordV1.write_cutover requires effective_from_counter or effective_from_lamport");
  }
  if (record.effectiveFromReplica && !hasCounter) {
    throw new Error("RevocationRecordV1.effective_from_replica requires effective_from_counter");
  }
}

export function issueTreecrdtRevocationRecordV1(opts: {
  issuerPrivateKey: Uint8Array;
  docId: string;
  tokenId: Uint8Array;
  mode: TreecrdtRevocationModeV1;
  revSeq: number;
  iat?: number;
  effectiveFromCounter?: number;
  effectiveFromReplica?: Uint8Array;
  effectiveFromLamport?: number;
}): Uint8Array {
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error("docId must not be empty");
  const tokenId = assertBytesLen(opts.tokenId, REVOCATION_TOKEN_ID_LEN, "tokenId");
  const mode = parseRevocationMode(opts.mode);
  const revSeq = assertInteger(opts.revSeq, "revSeq");
  const iat = opts.iat === undefined ? undefined : assertInteger(opts.iat, "iat");
  const effectiveFromCounter =
    opts.effectiveFromCounter === undefined
      ? undefined
      : assertInteger(opts.effectiveFromCounter, "effectiveFromCounter");
  const effectiveFromReplica =
    opts.effectiveFromReplica === undefined
      ? undefined
      : assertBytesLen(opts.effectiveFromReplica, ED25519_PUBLIC_KEY_LEN, "effectiveFromReplica");
  const effectiveFromLamport =
    opts.effectiveFromLamport === undefined
      ? undefined
      : assertInteger(opts.effectiveFromLamport, "effectiveFromLamport");

  validateModeFields(mode, { effectiveFromCounter, effectiveFromReplica, effectiveFromLamport });

  const claims = new Map<unknown, unknown>();
  claims.set("v", 1);
  claims.set("t", TREECRDT_REVOCATION_RECORD_V1_TAG);
  claims.set("doc_id", opts.docId);
  claims.set("token_id", tokenId);
  claims.set("mode", mode);
  claims.set("rev_seq", revSeq);
  if (iat !== undefined) claims.set("iat", iat);
  if (effectiveFromCounter !== undefined) claims.set("effective_from_counter", effectiveFromCounter);
  if (effectiveFromReplica !== undefined) claims.set("effective_from_replica", effectiveFromReplica);
  if (effectiveFromLamport !== undefined) claims.set("effective_from_lamport", effectiveFromLamport);

  return coseSign1Ed25519({ payload: encodeCbor(claims), privateKey: opts.issuerPrivateKey });
}

export async function verifyTreecrdtRevocationRecordV1(opts: {
  recordBytes: Uint8Array;
  issuerPublicKeys: Uint8Array[];
  expectedDocId?: string;
  nowSec?: () => number;
}): Promise<VerifiedTreecrdtRevocationRecordV1> {
  if (opts.issuerPublicKeys.length === 0) throw new Error("issuerPublicKeys is empty");

  let payloadBytes: Uint8Array | null = null;
  let verifiedBy: Uint8Array | null = null;
  for (const issuerPk of opts.issuerPublicKeys) {
    try {
      payloadBytes = await coseVerifySign1Ed25519({ bytes: opts.recordBytes, publicKey: issuerPk });
      verifiedBy = issuerPk;
      break;
    } catch {
      // continue
    }
  }
  if (!payloadBytes || !verifiedBy) throw new Error("revocation record signature verification failed");

  const decoded = decodeCbor(payloadBytes);
  const map = assertPayloadMap(decoded, "RevocationRecordV1");

  const v = mapGet(map, "v");
  if (v !== 1) throw new Error("RevocationRecordV1.v must be 1");
  const t = assertString(mapGet(map, "t"), "RevocationRecordV1.t");
  if (t !== TREECRDT_REVOCATION_RECORD_V1_TAG) throw new Error("RevocationRecordV1.t mismatch");

  const docId = assertString(mapGet(map, "doc_id"), "RevocationRecordV1.doc_id");
  if (opts.expectedDocId !== undefined && docId !== opts.expectedDocId) {
    throw new Error("RevocationRecordV1.doc_id mismatch");
  }
  const tokenId = assertBytesLen(mapGet(map, "token_id"), REVOCATION_TOKEN_ID_LEN, "RevocationRecordV1.token_id");
  const mode = parseRevocationMode(mapGet(map, "mode"));
  const revSeq = assertInteger(mapGet(map, "rev_seq"), "RevocationRecordV1.rev_seq");
  const iatRaw = mapGet(map, "iat");
  const iat = iatRaw === undefined ? undefined : assertInteger(iatRaw, "RevocationRecordV1.iat");

  const effectiveFromCounterRaw = mapGet(map, "effective_from_counter");
  const effectiveFromCounter =
    effectiveFromCounterRaw === undefined
      ? undefined
      : assertInteger(effectiveFromCounterRaw, "RevocationRecordV1.effective_from_counter");
  const effectiveFromReplicaRaw = mapGet(map, "effective_from_replica");
  const effectiveFromReplica =
    effectiveFromReplicaRaw === undefined
      ? undefined
      : assertBytesLen(effectiveFromReplicaRaw, ED25519_PUBLIC_KEY_LEN, "RevocationRecordV1.effective_from_replica");
  const effectiveFromLamportRaw = mapGet(map, "effective_from_lamport");
  const effectiveFromLamport =
    effectiveFromLamportRaw === undefined
      ? undefined
      : assertInteger(effectiveFromLamportRaw, "RevocationRecordV1.effective_from_lamport");

  validateModeFields(mode, { effectiveFromCounter, effectiveFromReplica, effectiveFromLamport });
  if (opts.nowSec && iat !== undefined && iat > opts.nowSec()) {
    throw new Error("RevocationRecordV1.iat is in the future");
  }

  return {
    docId,
    tokenId,
    mode,
    revSeq,
    ...(iat !== undefined ? { iat } : {}),
    ...(effectiveFromCounter !== undefined ? { effectiveFromCounter } : {}),
    ...(effectiveFromReplica !== undefined ? { effectiveFromReplica } : {}),
    ...(effectiveFromLamport !== undefined ? { effectiveFromLamport } : {}),
    issuerPublicKey: verifiedBy,
  };
}

export function createTreecrdtRevocationCapabilityV1(recordBytes: Uint8Array): Capability {
  return { name: TREECRDT_REVOCATION_CAPABILITY, value: base64urlEncode(recordBytes) };
}

export async function verifyTreecrdtRevocationCapabilityV1(opts: {
  capability: Capability;
  issuerPublicKeys: Uint8Array[];
  docId: string;
  nowSec?: () => number;
}): Promise<VerifiedTreecrdtRevocationRecordV1> {
  if (opts.capability.name !== TREECRDT_REVOCATION_CAPABILITY) {
    throw new Error(`unexpected capability: ${opts.capability.name}`);
  }
  const bytes = base64urlDecode(opts.capability.value);
  return verifyTreecrdtRevocationRecordV1({
    recordBytes: bytes,
    issuerPublicKeys: opts.issuerPublicKeys,
    expectedDocId: opts.docId,
    ...(opts.nowSec ? { nowSec: opts.nowSec } : {}),
  });
}

