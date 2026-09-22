import { decode, encode, rfc8949EncodeOptions } from 'cborg';
import { blake3 } from '@noble/hashes/blake3';
import { concatBytes, utf8ToBytes } from '@noble/hashes/utils';

import { assertAuthPublicKey, validateDocumentId } from './document-id.js';
import { verifyEd25519 } from './ed25519.js';

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
const PROTECTED_HEADER = encode(new Map([[1, -8]]));
const MAX_GRANT_BYTES = 1024;

function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((byte, i) => byte === b[i]);
}

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
  return encode(
    new Map<unknown, unknown>([
      [3, claims.docId],
      [4, claims.expiresAt],
      [
        8,
        new Map([
          [
            1,
            new Map<number, unknown>([
              [1, 1],
              [-1, 6],
              [-2, claims.replicaPublicKey],
            ]),
          ],
        ]),
      ],
      ['authority_pk', claims.authorityPublicKey],
    ]),
    rfc8949EncodeOptions,
  );
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
  const [header, unprotected, payload, signature] = envelope;
  if (
    !(header instanceof Uint8Array) ||
    !equalBytes(header, PROTECTED_HEADER) ||
    !(unprotected instanceof Map) ||
    unprotected.size !== 0 ||
    !(payload instanceof Uint8Array) ||
    !(signature instanceof Uint8Array) ||
    signature.length !== 64
  ) {
    throw new Error('Invalid owner grant COSE envelope');
  }
  if (!equalBytes(grant, encodeGrant(payload, signature))) {
    throw new Error('Non-canonical owner grant');
  }
  const claims = decode(payload, { useMaps: true });
  if (!(claims instanceof Map)) throw new Error('Invalid owner grant claims');
  const cnf = claims.get(8);
  const key = cnf instanceof Map ? cnf.get(1) : undefined;
  const replicaPublicKey = key instanceof Map ? key.get(-2) : undefined;
  const authorityPublicKey = claims.get('authority_pk');
  if (
    typeof claims.get(3) !== 'string' ||
    !(replicaPublicKey instanceof Uint8Array) ||
    !(authorityPublicKey instanceof Uint8Array)
  ) {
    throw new Error('Invalid owner grant identity claims');
  }
  const parsed: GrantClaims = {
    docId: claims.get(3),
    authorityPublicKey,
    replicaPublicKey,
    expiresAt: claims.get(4),
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
