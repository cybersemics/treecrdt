import { expect, test } from 'vitest';

import { generateTreecrdtDocPayloadKeyV1 } from '../dist/keystore.js';
import { decodeCbor, encodeCbor } from '../dist/internal/util.js';
import {
  createTreecrdtPayloadKeyringV1,
  decryptTreecrdtPayloadWithKeyring,
  encryptTreecrdtPayloadWithKeyring,
  rotateTreecrdtPayloadKeyringV1,
  upsertTreecrdtPayloadKeyringKeyV1,
} from '../dist/e2ee.js';

function rewriteEncryptedEnvelope(
  bytes: Uint8Array,
  rewrite: (envelope: Map<unknown, unknown>) => void,
): Uint8Array {
  const envelope = decodeCbor(bytes);
  if (!(envelope instanceof Map)) throw new Error('encrypted payload is not a CBOR map');
  rewrite(envelope);
  return encodeCbor(envelope);
}

test('e2ee v1 keyring: encrypts and decrypts with the active key id', async () => {
  const docId = 'doc-e2ee-kid';
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const keyring = createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: 'epoch-1' });

  const encrypted = await encryptTreecrdtPayloadWithKeyring({
    docId,
    keyring,
    plaintext: new TextEncoder().encode('payload with kid'),
  });
  const envelope = decodeCbor(encrypted);
  if (!(envelope instanceof Map)) throw new Error('encrypted payload is not a CBOR map');
  expect(envelope.get('v')).toBe(1);
  expect(envelope.get('t')).toBe('treecrdt/payload-encrypted/v1');
  expect(envelope.get('alg')).toBe('A256GCM');
  expect(envelope.get('kid')).toBe('epoch-1');

  const plaintext = await decryptTreecrdtPayloadWithKeyring({
    docId,
    keyring,
    ciphertext: encrypted,
  });
  expect(new TextDecoder().decode(plaintext)).toBe('payload with kid');
});

test('e2ee v1 keyring: authenticates the document id', async () => {
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId: 'doc-a' });
  const keyring = createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: 'epoch-1' });
  const encrypted = await encryptTreecrdtPayloadWithKeyring({
    docId: 'doc-a',
    keyring,
    plaintext: new TextEncoder().encode('hello'),
  });

  await expect(
    decryptTreecrdtPayloadWithKeyring({
      docId: 'doc-b',
      keyring,
      ciphertext: encrypted,
    }),
  ).rejects.toThrow();
});

test('e2ee v1 keyring: authenticates the key id even when aliases share key bytes', async () => {
  const docId = 'doc-e2ee-authenticated-kid';
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const sender = createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: 'epoch-1' });
  const encrypted = await encryptTreecrdtPayloadWithKeyring({
    docId,
    keyring: sender,
    plaintext: new TextEncoder().encode('bound to epoch-1'),
  });
  const relabeled = rewriteEncryptedEnvelope(encrypted, (envelope) => {
    envelope.set('kid', 'epoch-2');
  });
  const aliasedKeyring = {
    activeKid: 'epoch-2',
    keys: { 'epoch-1': payloadKey, 'epoch-2': payloadKey },
  };

  await expect(
    decryptTreecrdtPayloadWithKeyring({
      docId,
      keyring: aliasedKeyring,
      ciphertext: relabeled,
    }),
  ).rejects.toThrow();
});

test('e2ee v1 keyring: malformed envelopes fail closed', async () => {
  const docId = 'doc-e2ee-malformed';
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const keyring = createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: 'epoch-1' });
  const encrypted = await encryptTreecrdtPayloadWithKeyring({
    docId,
    keyring,
    plaintext: new TextEncoder().encode('not plaintext'),
  });

  for (const [rewrite, expected] of [
    [(envelope: Map<unknown, unknown>) => envelope.set('v', 2), /must be 1/],
    [(envelope: Map<unknown, unknown>) => envelope.set('t', 'not-a-treecrdt-envelope'), /mismatch/],
    [(envelope: Map<unknown, unknown>) => envelope.set('alg', 'A128GCM'), /unsupported/],
  ] as const) {
    await expect(
      decryptTreecrdtPayloadWithKeyring({
        docId,
        keyring,
        ciphertext: rewriteEncryptedEnvelope(encrypted, rewrite),
      }),
    ).rejects.toThrow(expected);
  }

  await expect(
    decryptTreecrdtPayloadWithKeyring({
      docId,
      keyring,
      ciphertext: new Uint8Array([1, 2, 3, 4]),
    }),
  ).rejects.toThrow();
});

test('e2ee v1 keyring: key ids are canonical and prototype-safe', async () => {
  const docId = 'doc-e2ee-canonical-kid';
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  expect(() => createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: ' epoch-1 ' })).toThrow(
    /canonical/,
  );

  const keyring = createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: '__proto__' });
  expect(Object.getPrototypeOf(keyring.keys)).toBeNull();
  const encrypted = await encryptTreecrdtPayloadWithKeyring({
    docId,
    keyring,
    plaintext: new TextEncoder().encode('prototype-safe'),
  });
  const plaintext = await decryptTreecrdtPayloadWithKeyring({
    docId,
    keyring,
    ciphertext: encrypted,
  });
  expect(new TextDecoder().decode(plaintext)).toBe('prototype-safe');

  const noncanonical = rewriteEncryptedEnvelope(encrypted, (envelope) => {
    envelope.set('kid', ' __proto__ ');
  });
  await expect(
    decryptTreecrdtPayloadWithKeyring({ docId, keyring, ciphertext: noncanonical }),
  ).rejects.toThrow(/canonical/);
});

test('e2ee v1 keyring: rejects ciphertext for an unavailable key id', async () => {
  const docId = 'doc-e2ee-missing-key';
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const sender = createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: 'epoch-1' });
  const encrypted = await encryptTreecrdtPayloadWithKeyring({
    docId,
    keyring: sender,
    plaintext: new TextEncoder().encode('secret'),
  });

  const { payloadKey: otherKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const receiver = createTreecrdtPayloadKeyringV1({ payloadKey: otherKey, activeKid: 'epoch-2' });
  await expect(
    decryptTreecrdtPayloadWithKeyring({
      docId,
      keyring: receiver,
      ciphertext: encrypted,
    }),
  ).rejects.toThrow(/payload key not found: epoch-1/);
});

test('e2ee v1 keyring: retained keys decrypt payloads written before rotation', async () => {
  const docId = 'doc-e2ee-rotate';
  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
  const keyringV1 = createTreecrdtPayloadKeyringV1({ payloadKey, activeKid: 'epoch-1' });
  const oldEncrypted = await encryptTreecrdtPayloadWithKeyring({
    docId,
    keyring: keyringV1,
    plaintext: new TextEncoder().encode('before rotate'),
  });

  const { keyring: keyringV2 } = rotateTreecrdtPayloadKeyringV1({
    keyring: keyringV1,
    nextKid: 'epoch-2',
  });
  const newEncrypted = await encryptTreecrdtPayloadWithKeyring({
    docId,
    keyring: keyringV2,
    plaintext: new TextEncoder().encode('after rotate'),
  });

  const [oldPlaintext, newPlaintext] = await Promise.all([
    decryptTreecrdtPayloadWithKeyring({ docId, keyring: keyringV2, ciphertext: oldEncrypted }),
    decryptTreecrdtPayloadWithKeyring({ docId, keyring: keyringV2, ciphertext: newEncrypted }),
  ]);
  expect(new TextDecoder().decode(oldPlaintext)).toBe('before rotate');
  expect(new TextDecoder().decode(newPlaintext)).toBe('after rotate');

  expect(() => rotateTreecrdtPayloadKeyringV1({ keyring: keyringV2, nextKid: 'epoch-2' })).toThrow(
    /already exists/,
  );

  const currentKey = keyringV2.keys['epoch-2'];
  if (!currentKey) throw new Error('rotated key is missing');
  expect(
    upsertTreecrdtPayloadKeyringKeyV1({
      keyring: keyringV2,
      kid: 'epoch-2',
      payloadKey: currentKey,
    }).keys['epoch-2'],
  ).toEqual(currentKey);

  expect(() =>
    upsertTreecrdtPayloadKeyringKeyV1({
      keyring: keyringV2,
      kid: 'epoch-2',
      payloadKey: new Uint8Array(32).fill(0xff),
    }),
  ).toThrow(/different key bytes/);
});
