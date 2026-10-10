import { deriveDocumentId } from './document-id.js';
import { encodeGrant, encodeGrantPayload, grantSignatureInput, type Grant } from './grant.js';

declare const keyHandleBrand: unique symbol;
type KeyHandle<Role extends 'authority' | 'replica'> = {
  readonly [keyHandleBrand]: true;
  readonly role: Role;
  readonly docId: string;
};
export type AuthorityKey = KeyHandle<'authority'>;
export type ReplicaKey = KeyHandle<'replica'>;
type Key = KeyHandle<'authority' | 'replica'>;

export interface AuthKeyProvider {
  readonly storage: 'memory';
  createAuthorityKey(): Promise<AuthorityKey>;
  createReplicaKey(docId: string): Promise<ReplicaKey>;
  getPublicKey(key: Key): Promise<Uint8Array>;
  deleteKey(key: Key): Promise<void>;
  issueGrant(opts: {
    authorityKey: AuthorityKey;
    replicaPublicKey: Uint8Array;
    expiresAt: number;
  }): Promise<Grant>;
}

/** Explicitly ephemeral: losing this provider loses its keys. Requires Web Crypto Ed25519. */
export function createMemoryAuthKeyProvider(): AuthKeyProvider {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) throw new Error('Web Crypto is unavailable');
  const keys = new WeakMap<Key, { privateKey: CryptoKey; publicKey: Uint8Array }>();

  function lookup(key: Key) {
    const value = keys.get(key);
    if (!value) throw new Error('Unknown or deleted key reference');
    return value;
  }

  async function createKey<Role extends Key['role']>(
    role: Role,
    docId?: string,
  ): Promise<KeyHandle<Role>> {
    const pair = await subtle.generateKey('Ed25519', false, ['sign', 'verify']);
    const publicKey = new Uint8Array(await subtle.exportKey('raw', pair.publicKey));
    const ref = Object.freeze({
      role,
      docId: docId ?? deriveDocumentId(publicKey),
    }) as KeyHandle<Role>;
    keys.set(ref, { privateKey: pair.privateKey, publicKey });
    return ref;
  }

  return {
    storage: 'memory',
    createAuthorityKey() {
      return createKey('authority');
    },
    createReplicaKey(docId) {
      return createKey('replica', docId);
    },
    async getPublicKey(key) {
      return new Uint8Array(lookup(key).publicKey);
    },
    async deleteKey(key) {
      keys.delete(key);
    },
    async issueGrant({ authorityKey, replicaPublicKey, expiresAt }) {
      const key = lookup(authorityKey);
      if (authorityKey.role !== 'authority') throw new Error('Expected authority key');
      const payload = encodeGrantPayload({
        docId: authorityKey.docId,
        authorityPublicKey: key.publicKey,
        replicaPublicKey,
        expiresAt,
      });
      const signature = new Uint8Array(
        await subtle.sign('Ed25519', key.privateKey, new Uint8Array(grantSignatureInput(payload))),
      );
      // Deletion while signing must not publish another grant from the deleted key.
      lookup(authorityKey);
      return encodeGrant(payload, signature);
    },
  };
}
