import { Point } from '@noble/ed25519';
import { blake3 } from '@noble/hashes/blake3';
import { concatBytes, utf8ToBytes } from '@noble/hashes/utils';

import { base64urlDecode, base64urlEncode } from './base64url.js';

const DOCUMENT_ID_PREFIX = 'treecrdt:doc:v1:';
const DOCUMENT_ID_DOMAIN = utf8ToBytes('treecrdt/document-id/v1\0');

/** @internal */
export function assertAuthPublicKey(bytes: Uint8Array): void {
  if (bytes.length !== 32 || Point.fromBytes(bytes, false).isSmallOrder()) {
    throw new Error('Expected a canonical, non-small-order Ed25519 public key');
  }
}

/** @internal */
export function assertDocumentId(docId: string): void {
  const digest = docId.slice(DOCUMENT_ID_PREFIX.length);
  if (
    !docId.startsWith(DOCUMENT_ID_PREFIX) ||
    !/^[A-Za-z0-9_-]{43}$/.test(digest) ||
    base64urlEncode(base64urlDecode(digest)) !== digest
  ) {
    throw new Error('Invalid document ID');
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
  assertDocumentId(docId);
  if (docId !== deriveDocumentId(authorityPublicKey)) {
    throw new Error('Document authority does not match document ID');
  }
}
