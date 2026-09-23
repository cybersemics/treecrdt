import { decode, encode, rfc8949EncodeOptions } from 'cborg';
import { blake3 } from '@noble/hashes/blake3';
import { concatBytes, utf8ToBytes } from '@noble/hashes/utils';

import { assertAuthPublicKey, validateDocumentId } from './document-id.js';
import { verifyEd25519 } from './ed25519.js';
import { equalBytes } from './internal/bytes.js';

declare const grantBrand: unique symbol;
/** Opaque canonical COSE bytes. Possession alone does not establish authorization. */
export type Grant = Uint8Array & { readonly [grantBrand]: true };

type GrantClaims = {
  docId: string;
  authorityPublicKey: Uint8Array;
  replicaPublicKey: Uint8Array;
  expiresAt: number;
};

const GRANT_DOMAIN = utf8ToBytes('treecrdt/owner-grant/v1');
const GRANT_ID_DOMAIN = utf8ToBytes('treecrdt/owner-grant-id/v1\0');
// COSE Ed25519 algorithm: https://www.rfc-editor.org/rfc/rfc9864.html#section-2.2
const COSE_HEADER = { algorithm: 1, ed25519: -19 };
// CWT and confirmation fields: https://www.rfc-editor.org/rfc/rfc8747.html#section-3.2
const CWT = { audience: 3, expiration: 4, confirmation: 8 };
const CONFIRMATION = { coseKey: 1 };
// Octet key pair fields: https://www.rfc-editor.org/rfc/rfc9053.html#section-7.2
const COSE_KEY = { type: 1, okp: 1, curve: -1, ed25519: 6, publicKey: -2 };
const PROTECTED_HEADER = encode(new Map([[COSE_HEADER.algorithm, COSE_HEADER.ed25519]]));
const MAX_GRANT_BYTES = 1024;

/** @internal */
export function encodeGrantPayload(claims: GrantClaims): Uint8Array {
  validateDocumentId(claims.docId, claims.authorityPublicKey);
  assertAuthPublicKey(claims.replicaPublicKey);
  if (equalBytes(claims.authorityPublicKey, claims.replicaPublicKey)) {
    throw new Error('Authority and replica must use separate keys');
  }
  if (!Number.isSafeInteger(claims.expiresAt) || claims.expiresAt < 0) {
    throw new Error('expiresAt must be a safe non-negative integer');
  }
  const replicaKey = new Map<number, unknown>([
    [COSE_KEY.type, COSE_KEY.okp],
    [COSE_KEY.curve, COSE_KEY.ed25519],
    [COSE_KEY.publicKey, claims.replicaPublicKey],
  ]);
  const confirmation = new Map([[CONFIRMATION.coseKey, replicaKey]]);
  const payload = new Map<unknown, unknown>([
    [CWT.audience, claims.docId],
    [CWT.expiration, claims.expiresAt],
    [CWT.confirmation, confirmation],
    ['authority_pk', claims.authorityPublicKey],
  ]);
  return encode(payload, rfc8949EncodeOptions);
}

/** @internal */
export function grantSignatureInput(payload: Uint8Array): Uint8Array {
  return encode(['Signature1', PROTECTED_HEADER, GRANT_DOMAIN, payload], rfc8949EncodeOptions);
}

/** @internal */
export function encodeGrant(payload: Uint8Array, signature: Uint8Array): Grant {
  return encode([PROTECTED_HEADER, new Map(), payload, signature], rfc8949EncodeOptions) as Grant;
}

/** Verifies a direct grant's identity, signature and expiry; revocation is checked separately. */
export async function verifyGrant(opts: {
  grant: Uint8Array;
  docId: string;
  replicaPublicKey: Uint8Array;
  nowSec?: number;
}): Promise<GrantClaims & { grantId: Uint8Array }> {
  if (opts.grant.length > MAX_GRANT_BYTES) throw new Error('Owner grant exceeds 1024 bytes');
  const grant = new Uint8Array(opts.grant);
  const nowSec = opts.nowSec ?? Math.floor(Date.now() / 1000);
  if (!Number.isSafeInteger(nowSec) || nowSec < 0) throw new Error('Invalid verification time');

  const envelope = decode(grant, { useMaps: true });
  if (!Array.isArray(envelope) || envelope.length !== 4) throw new Error('Invalid owner grant');
  const [, , payload, signature] = envelope;
  // Re-encoding fixes both headers and enforces canonical envelope bytes.
  if (
    !(payload instanceof Uint8Array) ||
    !(signature instanceof Uint8Array) ||
    signature.length !== 64 ||
    !equalBytes(grant, encodeGrant(payload, signature))
  ) {
    throw new Error('Invalid owner grant COSE envelope');
  }
  const claims = decode(payload, { useMaps: true });
  if (!(claims instanceof Map)) throw new Error('Invalid owner grant claims');
  const confirmation = claims.get(CWT.confirmation);
  const replicaKey =
    confirmation instanceof Map ? confirmation.get(CONFIRMATION.coseKey) : undefined;
  const replicaPublicKey =
    replicaKey instanceof Map ? replicaKey.get(COSE_KEY.publicKey) : undefined;
  const authorityPublicKey = claims.get('authority_pk');
  if (
    typeof claims.get(CWT.audience) !== 'string' ||
    !(replicaPublicKey instanceof Uint8Array) ||
    !(authorityPublicKey instanceof Uint8Array)
  ) {
    throw new Error('Invalid owner grant identity claims');
  }
  const parsed: GrantClaims = {
    docId: claims.get(CWT.audience),
    authorityPublicKey,
    replicaPublicKey,
    expiresAt: claims.get(CWT.expiration),
  };
  // Exact re-encoding enforces canonical bytes and rejects extra or alternate claims.
  if (!equalBytes(payload, encodeGrantPayload(parsed))) {
    throw new Error('Unsupported owner grant claims');
  }
  if (parsed.docId !== opts.docId || !equalBytes(replicaPublicKey, opts.replicaPublicKey)) {
    throw new Error('Owner grant document or replica mismatch');
  }
  if (nowSec >= parsed.expiresAt) throw new Error('Owner grant expired');
  if (!(await verifyEd25519(signature, grantSignatureInput(payload), authorityPublicKey))) {
    throw new Error('Invalid owner grant signature');
  }
  return {
    ...parsed,
    grantId: blake3(concatBytes(GRANT_ID_DOMAIN, grant)).slice(0, 16),
  };
}
