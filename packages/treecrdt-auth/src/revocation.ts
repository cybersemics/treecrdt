import { decode as cborDecode, encode as cborEncode, rfc8949EncodeOptions } from "cborg";

import type { Capability } from "@treecrdt/sync";

import { base64urlDecode, base64urlEncode } from "./base64url.js";
import { coseSign1Ed25519, coseVerifySign1Ed25519 } from "./cose.js";

const TREECRDT_REVOCATION_RECORD_V1_TAG = "treecrdt/revocation/v1";
const REVOCATION_TOKEN_ID_LEN = 16;
const ED25519_PUBLIC_KEY_LEN = 32;

// Canonical claim keys for signed revocation records (v1).
// These keys are wire-level protocol names and must stay stable for interop.
const REVOCATION_V1_CLAIM = {
  VERSION: "v",
  TYPE_TAG: "t",
  DOC_ID: "doc_id",
  TOKEN_ID: "token_id",
  MODE: "mode",
  REV_SEQ: "rev_seq",
  IAT: "iat",
  EFFECTIVE_FROM_COUNTER: "effective_from_counter",
  EFFECTIVE_FROM_REPLICA: "effective_from_replica",
} as const;

const REVOCATION_RECORD_V1_CONTEXT = "RevocationRecordV1";

const REVOCATION_V1_ALLOWED_CLAIMS = new Set<string>([
  REVOCATION_V1_CLAIM.VERSION,
  REVOCATION_V1_CLAIM.TYPE_TAG,
  REVOCATION_V1_CLAIM.DOC_ID,
  REVOCATION_V1_CLAIM.TOKEN_ID,
  REVOCATION_V1_CLAIM.MODE,
  REVOCATION_V1_CLAIM.REV_SEQ,
  REVOCATION_V1_CLAIM.IAT,
  REVOCATION_V1_CLAIM.EFFECTIVE_FROM_COUNTER,
  REVOCATION_V1_CLAIM.EFFECTIVE_FROM_REPLICA,
]);

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

function assertNoUnknownClaims(payloadMap: Map<unknown, unknown>, ctx: string): void {
  for (const key of payloadMap.keys()) {
    if (typeof key !== "string") throw new Error(`${ctx} contains non-string claim key`);
    if (!REVOCATION_V1_ALLOWED_CLAIMS.has(key)) throw new Error(`${ctx} contains unknown claim: ${key}`);
  }
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
}) {
  if (mode === "hard") return;

  const hasCounter = record.effectiveFromCounter !== undefined;
  if (!hasCounter) {
    throw new Error("RevocationRecordV1.write_cutover requires effective_from_counter");
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

  validateModeFields(mode, { effectiveFromCounter, effectiveFromReplica });

  const claims = new Map<unknown, unknown>();
  claims.set(REVOCATION_V1_CLAIM.VERSION, 1);
  claims.set(REVOCATION_V1_CLAIM.TYPE_TAG, TREECRDT_REVOCATION_RECORD_V1_TAG);
  claims.set(REVOCATION_V1_CLAIM.DOC_ID, opts.docId);
  claims.set(REVOCATION_V1_CLAIM.TOKEN_ID, tokenId);
  claims.set(REVOCATION_V1_CLAIM.MODE, mode);
  claims.set(REVOCATION_V1_CLAIM.REV_SEQ, revSeq);
  if (iat !== undefined) claims.set(REVOCATION_V1_CLAIM.IAT, iat);
  if (effectiveFromCounter !== undefined) claims.set(REVOCATION_V1_CLAIM.EFFECTIVE_FROM_COUNTER, effectiveFromCounter);
  if (effectiveFromReplica !== undefined) claims.set(REVOCATION_V1_CLAIM.EFFECTIVE_FROM_REPLICA, effectiveFromReplica);

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
  const map = assertPayloadMap(decoded, REVOCATION_RECORD_V1_CONTEXT);
  assertNoUnknownClaims(map, REVOCATION_RECORD_V1_CONTEXT);

  const v = mapGet(map, REVOCATION_V1_CLAIM.VERSION);
  if (v !== 1) throw new Error(`${REVOCATION_RECORD_V1_CONTEXT}.${REVOCATION_V1_CLAIM.VERSION} must be 1`);
  const t = assertString(
    mapGet(map, REVOCATION_V1_CLAIM.TYPE_TAG),
    `${REVOCATION_RECORD_V1_CONTEXT}.${REVOCATION_V1_CLAIM.TYPE_TAG}`
  );
  if (t !== TREECRDT_REVOCATION_RECORD_V1_TAG) throw new Error("RevocationRecordV1.t mismatch");

  const docId = assertString(
    mapGet(map, REVOCATION_V1_CLAIM.DOC_ID),
    `${REVOCATION_RECORD_V1_CONTEXT}.${REVOCATION_V1_CLAIM.DOC_ID}`
  );
  if (opts.expectedDocId !== undefined && docId !== opts.expectedDocId) {
    throw new Error("RevocationRecordV1.doc_id mismatch");
  }
  const tokenId = assertBytesLen(
    mapGet(map, REVOCATION_V1_CLAIM.TOKEN_ID),
    REVOCATION_TOKEN_ID_LEN,
    `${REVOCATION_RECORD_V1_CONTEXT}.${REVOCATION_V1_CLAIM.TOKEN_ID}`
  );
  const mode = parseRevocationMode(mapGet(map, REVOCATION_V1_CLAIM.MODE));
  const revSeq = assertInteger(
    mapGet(map, REVOCATION_V1_CLAIM.REV_SEQ),
    `${REVOCATION_RECORD_V1_CONTEXT}.${REVOCATION_V1_CLAIM.REV_SEQ}`
  );
  const iatRaw = mapGet(map, REVOCATION_V1_CLAIM.IAT);
  const iat =
    iatRaw === undefined
      ? undefined
      : assertInteger(iatRaw, `${REVOCATION_RECORD_V1_CONTEXT}.${REVOCATION_V1_CLAIM.IAT}`);

  const effectiveFromCounterRaw = mapGet(map, REVOCATION_V1_CLAIM.EFFECTIVE_FROM_COUNTER);
  const effectiveFromCounter =
    effectiveFromCounterRaw === undefined
      ? undefined
      : assertInteger(
          effectiveFromCounterRaw,
          `${REVOCATION_RECORD_V1_CONTEXT}.${REVOCATION_V1_CLAIM.EFFECTIVE_FROM_COUNTER}`
        );
  const effectiveFromReplicaRaw = mapGet(map, REVOCATION_V1_CLAIM.EFFECTIVE_FROM_REPLICA);
  const effectiveFromReplica =
    effectiveFromReplicaRaw === undefined
      ? undefined
      : assertBytesLen(
          effectiveFromReplicaRaw,
          ED25519_PUBLIC_KEY_LEN,
          `${REVOCATION_RECORD_V1_CONTEXT}.${REVOCATION_V1_CLAIM.EFFECTIVE_FROM_REPLICA}`
        );

  validateModeFields(mode, { effectiveFromCounter, effectiveFromReplica });
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
