import { assertDocumentId, deriveDocumentId } from './document-id.js';
import {
  encodeOwnerGrant,
  encodeOwnerGrantPayload,
  ownerGrantSignatureInput,
  type OwnerGrant,
} from './owner-grant.js';

declare const keyRefBrand: unique symbol;
export type DocumentAuthorityKeyRef = {
  readonly [keyRefBrand]: true;
  readonly role: 'document-authority';
  readonly docId: string;
};
export type ReplicaKeyRef = {
  readonly [keyRefBrand]: true;
  readonly role: 'replica';
  readonly docId: string;
};
type KeyRef = DocumentAuthorityKeyRef | ReplicaKeyRef;

export interface AuthKeyProvider {
  readonly storage: 'memory';
  createDocumentAuthorityKey(): Promise<DocumentAuthorityKeyRef>;
  createReplicaKey(docId: string): Promise<ReplicaKeyRef>;
  getPublicKey(key: KeyRef): Promise<Uint8Array>;
  deleteKey(key: KeyRef): Promise<void>;
  issueDirectOwnerGrant(opts: {
    authorityKey: DocumentAuthorityKeyRef;
    replicaPublicKey: Uint8Array;
    expiresAt: number;
  }): Promise<OwnerGrant>;
}

/** Explicitly ephemeral: losing this provider loses its keys. Requires Web Crypto Ed25519. */
export function createMemoryAuthKeyProvider(): AuthKeyProvider {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) throw new Error('Web Crypto is unavailable');
  const keys = new WeakMap<KeyRef, { privateKey: CryptoKey; publicKey: Uint8Array }>();

  function lookup(key: KeyRef) {
    const value = keys.get(key);
    if (!value) throw new Error('Unknown or deleted key reference');
    return value;
  }

  async function createKey(role: KeyRef['role'], docId?: string): Promise<KeyRef> {
    const pair = await subtle.generateKey('Ed25519', false, ['sign', 'verify']);
    const publicKey = new Uint8Array(await subtle.exportKey('raw', pair.publicKey));
    const ref = Object.freeze({ role, docId: docId ?? deriveDocumentId(publicKey) }) as KeyRef;
    keys.set(ref, { privateKey: pair.privateKey, publicKey });
    return ref;
  }

  return {
    storage: 'memory',
    async createDocumentAuthorityKey() {
      return (await createKey('document-authority')) as DocumentAuthorityKeyRef;
    },
    async createReplicaKey(docId) {
      assertDocumentId(docId);
      return (await createKey('replica', docId)) as ReplicaKeyRef;
    },
    async getPublicKey(key) {
      return new Uint8Array(lookup(key).publicKey);
    },
    async deleteKey(key) {
      keys.delete(key);
    },
    async issueDirectOwnerGrant({ authorityKey, replicaPublicKey, expiresAt }) {
      const key = lookup(authorityKey);
      if (authorityKey.role !== 'document-authority') throw new Error('Expected authority key');
      const payload = encodeOwnerGrantPayload({
        docId: authorityKey.docId,
        authorityPublicKey: key.publicKey,
        replicaPublicKey,
        expiresAt,
      });
      const signature = new Uint8Array(
        await subtle.sign(
          'Ed25519',
          key.privateKey,
          new Uint8Array(ownerGrantSignatureInput(payload)),
        ),
      );
      // Deletion while signing must not publish another grant from the deleted key.
      lookup(authorityKey);
      return encodeOwnerGrant(payload, signature);
    },
  };
}
