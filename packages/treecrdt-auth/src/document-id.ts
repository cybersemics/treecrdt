import { Point } from '@noble/ed25519';
import { blake3 } from '@noble/hashes/blake3';
import { concatBytes, utf8ToBytes } from '@noble/hashes/utils';

import { base64urlEncode } from './base64url.js';

const DOCUMENT_ID_PREFIX = 'treecrdt:doc:v1:';
const DOCUMENT_ID_DOMAIN = utf8ToBytes('treecrdt/document-id/v1\0');

/** @internal */
export function assertAuthPublicKey(bytes: Uint8Array): void {
  if (bytes.length !== 32 || Point.fromBytes(bytes, false).isSmallOrder()) {
    throw new Error('Expected a canonical, non-small-order Ed25519 public key');
  }
}

export function deriveDocumentId(authorityPublicKey: Uint8Array): string {
  assertAuthPublicKey(authorityPublicKey);
  return (
    DOCUMENT_ID_PREFIX +
    base64urlEncode(blake3(concatBytes(DOCUMENT_ID_DOMAIN, authorityPublicKey)))
  );
}

/** Throws if the document ID is malformed or belongs to a different authority. */
export function validateDocumentId(docId: string, authorityPublicKey: Uint8Array): void {
  if (docId !== deriveDocumentId(authorityPublicKey)) {
    throw new Error('Document authority does not match document ID');
  }
}
