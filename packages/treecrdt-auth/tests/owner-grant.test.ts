import { readFileSync } from 'node:fs';
import { runInNewContext } from 'node:vm';
import { decode, encode, rfc8949EncodeOptions } from 'cborg';
import { afterEach, expect, test, vi } from 'vitest';

import {
  createMemoryAuthKeyProvider,
  deriveDocumentId,
  validateDocumentId,
  verifyDirectOwnerGrant,
  type DocumentAuthorityKeyRef,
} from '../dist/index.js';

const vector: {
  authorityPublicKeyHex: string;
  replicaPublicKeyHex: string;
  otherAuthorityPublicKeyHex: string;
  docId: string;
  otherDocId: string;
  expiresAt: number;
  nowSec: number;
  grantHex: string;
  ownerGrantIdHex: string;
  rejected: { name: string; grantHex: string }[];
} = JSON.parse(
  readFileSync(new URL('../../../fixtures/owner-grant-v1.json', import.meta.url), 'utf8'),
);
const fromHex = (hex: string) => new Uint8Array(Buffer.from(hex, 'hex'));
const authorityPublicKey = fromHex(vector.authorityPublicKeyHex);
const replicaPublicKey = fromHex(vector.replicaPublicKeyHex);
const grant = fromHex(vector.grantHex);
const verification = { docId: vector.docId, replicaPublicKey, nowSec: vector.nowSec };

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

test('document identity and direct grant match the shared wire vector', async () => {
  expect(deriveDocumentId(authorityPublicKey)).toBe(vector.docId);
  expect(() => validateDocumentId(vector.docId, authorityPublicKey)).not.toThrow();
  expect(deriveDocumentId(fromHex(vector.otherAuthorityPublicKeyHex))).toBe(vector.otherDocId);
  expect(() =>
    validateDocumentId(vector.docId, fromHex(vector.otherAuthorityPublicKeyHex)),
  ).toThrow(/authority.*match/i);

  const padded = new Uint8Array(grant.length + 2);
  padded.set(grant, 1);
  for (const bytes of [
    grant,
    padded.subarray(1, -1),
    Buffer.from(grant),
    runInNewContext('new Uint8Array(bytes)', { bytes: [...grant] }),
  ]) {
    const verified = await verifyDirectOwnerGrant({ ...verification, grant: bytes });
    expect(verified).toEqual({
      docId: vector.docId,
      authorityPublicKey,
      replicaPublicKey,
      expiresAt: vector.expiresAt,
      ownerGrantId: fromHex(vector.ownerGrantIdHex),
    });
  }
});

test('document IDs reject malformed and non-canonical forms', () => {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
  const alias = vector.docId.slice(0, -1) + alphabet[alphabet.indexOf(vector.docId.at(-1)!) + 1];
  for (const docId of [
    '',
    'doc',
    vector.docId + '=',
    vector.docId + ' ',
    vector.docId.replace(':v1:', ':v2:'),
    alias,
  ]) {
    expect(() => validateDocumentId(docId, authorityPublicKey)).toThrow(/Invalid document ID/);
  }
  for (const key of [new Uint8Array(31), new Uint8Array(32), new Uint8Array(32).fill(255)]) {
    expect(() => deriveDocumentId(key)).toThrow();
  }
});

test.each(vector.rejected)('rejects shared vector: $name', async ({ grantHex }) => {
  await expect(
    verifyDirectOwnerGrant({ ...verification, grant: fromHex(grantHex) }),
  ).rejects.toThrow();
});

test('a valid signature only authorizes its exact document and replica before expiry', async () => {
  for (const change of [
    { docId: vector.otherDocId },
    { replicaPublicKey: authorityPublicKey },
    { nowSec: vector.expiresAt },
    { nowSec: Number.NaN },
  ]) {
    await expect(verifyDirectOwnerGrant({ ...verification, grant, ...change })).rejects.toThrow();
  }
  vi.spyOn(Date, 'now').mockReturnValue(vector.expiresAt * 1000);
  await expect(
    verifyDirectOwnerGrant({ ...verification, grant, nowSec: undefined }),
  ).rejects.toThrow(/expired/);
});

test('rejects unsigned headers, duplicate claims, wrong algorithms and invalid signatures', async () => {
  const envelope = decode(grant, { useMaps: true }) as unknown[];
  const payload = envelope[2] as Uint8Array;
  // The canonical payload has five claims; append a second role claim.
  const duplicate = new Uint8Array([
    0xa6,
    ...payload.subarray(1),
    ...encode('role'),
    ...encode('owner'),
  ]);
  for (const altered of [
    [envelope[0], new Map([['authority_pk', authorityPublicKey]]), payload, envelope[3]],
    [encode(new Map([[1, -7]])), envelope[1], payload, envelope[3]],
    [envelope[0], envelope[1], duplicate, envelope[3]],
    [envelope[0], envelope[1], payload, new Uint8Array(64)],
  ]) {
    await expect(
      verifyDirectOwnerGrant({
        ...verification,
        grant: encode(altered, rfc8949EncodeOptions),
      }),
    ).rejects.toThrow();
  }
  await expect(
    verifyDirectOwnerGrant({ ...verification, grant: new Uint8Array(1025) }),
  ).rejects.toThrow(/exceeds/);
});

test('verification snapshots the grant and expected identity across awaits', async () => {
  const opts = {
    ...verification,
    grant: new Uint8Array(grant),
    replicaPublicKey: new Uint8Array(replicaPublicKey),
  };
  const pending = verifyDirectOwnerGrant(opts);
  opts.grant.fill(0);
  opts.replicaPublicKey.fill(0);
  opts.docId = vector.otherDocId;
  const result = await pending;
  expect(result.docId).toBe(vector.docId);
  expect(result.replicaPublicKey).toEqual(replicaPublicKey);
  expect(result.ownerGrantId).toEqual(fromHex(vector.ownerGrantIdHex));
});

test('the memory provider creates separate opaque keys and issues a verifiable direct grant', async () => {
  const generate = vi.spyOn(crypto.subtle, 'generateKey');
  const provider = createMemoryAuthKeyProvider();
  const authorityKey = await provider.createDocumentAuthorityKey();
  const replicaKey = await provider.createReplicaKey(authorityKey.docId);
  const replicaPublicKey = await provider.getPublicKey(replicaKey);
  const expectedReplica = new Uint8Array(replicaPublicKey);
  const pending = provider.issueDirectOwnerGrant({
    authorityKey,
    replicaPublicKey,
    expiresAt: vector.expiresAt,
  });
  replicaPublicKey.fill(0);
  const issued = await pending;
  const verified = await verifyDirectOwnerGrant({
    grant: issued,
    docId: authorityKey.docId,
    replicaPublicKey: expectedReplica,
    nowSec: vector.nowSec,
  });
  expect(verified.authorityPublicKey).toEqual(await provider.getPublicKey(authorityKey));
  expect(await provider.getPublicKey(replicaKey)).toEqual(expectedReplica);
  expect(verified.authorityPublicKey).not.toEqual(expectedReplica);
  expect(provider.storage).toBe('memory');
  expect(Object.isFrozen(authorityKey)).toBe(true);
  expect(Object.keys(authorityKey).sort()).toEqual(['docId', 'role']);
  for (const result of generate.mock.results) {
    const pair = (await result.value) as CryptoKeyPair;
    expect(pair.privateKey.extractable).toBe(false);
    await expect(crypto.subtle.exportKey('pkcs8', pair.privateKey)).rejects.toThrow();
  }
});

test('provider rejects forged, foreign, wrong-role and deleted key handles', async () => {
  const provider = createMemoryAuthKeyProvider();
  const authorityKey = await provider.createDocumentAuthorityKey();
  const replicaKey = await provider.createReplicaKey(authorityKey.docId);
  const foreign = await createMemoryAuthKeyProvider().createDocumentAuthorityKey();
  for (const key of [{ ...authorityKey }, foreign, replicaKey]) {
    await expect(
      provider.issueDirectOwnerGrant({
        authorityKey: key as DocumentAuthorityKeyRef,
        replicaPublicKey,
        expiresAt: vector.expiresAt,
      }),
    ).rejects.toThrow(/reference|authority key/);
  }
  await expect(
    provider.issueDirectOwnerGrant({
      authorityKey,
      replicaPublicKey: await provider.getPublicKey(authorityKey),
      expiresAt: vector.expiresAt,
    }),
  ).rejects.toThrow(/separate keys/);
  await expect(provider.createReplicaKey('arbitrary-doc')).rejects.toThrow(/document ID/);
  await provider.deleteKey(authorityKey);
  await provider.deleteKey(authorityKey);
  await expect(provider.getPublicKey(authorityKey)).rejects.toThrow(/deleted/);
});

test('deleting an authority during signing prevents grant publication', async () => {
  const provider = createMemoryAuthKeyProvider();
  const authorityKey = await provider.createDocumentAuthorityKey();
  const sign = crypto.subtle.sign.bind(crypto.subtle);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  vi.spyOn(crypto.subtle, 'sign').mockImplementation(async (...args) => {
    await gate;
    return sign(...args);
  });
  const pending = provider.issueDirectOwnerGrant({
    authorityKey,
    replicaPublicKey,
    expiresAt: vector.expiresAt,
  });
  const rejected = expect(pending).rejects.toThrow(/deleted/);
  await provider.deleteKey(authorityKey);
  release();
  await rejected;
});

test('provider exposes unavailable Web Crypto instead of falling back to extractable keys', () => {
  vi.stubGlobal('crypto', undefined);
  expect(() => createMemoryAuthKeyProvider()).toThrow(/unavailable/);
});
