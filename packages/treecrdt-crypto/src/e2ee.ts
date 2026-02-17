import {
  aesGcmDecrypt,
  aesGcmEncrypt,
  assertBytes,
  assertLen,
  assertMap,
  assertString,
  decodeCbor,
  encodeCbor,
  mapGet,
  randomBytes,
} from './internal/util.js';

const DOC_PAYLOAD_KEY_LEN = 32;
const AES_GCM_NONCE_LEN = 12;
const ENCRYPTED_PAYLOAD_V1_TAG = 'treecrdt/payload-encrypted/v1';
const ENCRYPTED_PAYLOAD_ALG = 'A256GCM';
const PAYLOAD_KEY_ID_MAX_LEN = 128;
const PAYLOAD_KEY_ID_PATTERN = /^[A-Za-z0-9._:-]+$/;

function payloadAadV1(docId: string, keyId: string): Uint8Array {
  return encodeCbor([ENCRYPTED_PAYLOAD_V1_TAG, 1, ENCRYPTED_PAYLOAD_ALG, docId, keyId]);
}

function bytesToHex(bytes: Uint8Array): string {
  let out = '';
  for (const b of bytes) out += b.toString(16).padStart(2, '0');
  return out;
}

function randomPayloadKeyIdV1(): string {
  return `k${bytesToHex(randomBytes(8))}`;
}

function assertDocId(docId: string): string {
  if (!docId || docId.trim().length === 0) throw new Error('docId must not be empty');
  return docId;
}

function assertPayloadKeyIdV1(kid: string, field: string): string {
  if (typeof kid !== 'string') throw new Error(`${field} must be a string`);
  if (kid.length === 0) throw new Error(`${field} must not be empty`);
  if (kid !== kid.trim()) throw new Error(`${field} must be canonical (no surrounding space)`);
  if (kid.length > PAYLOAD_KEY_ID_MAX_LEN) {
    throw new Error(`${field} too long (max ${PAYLOAD_KEY_ID_MAX_LEN})`);
  }
  if (!PAYLOAD_KEY_ID_PATTERN.test(kid)) {
    throw new Error(`${field} contains unsupported characters`);
  }
  return kid;
}

function emptyPayloadKeys(): Record<string, Uint8Array> {
  return Object.create(null) as Record<string, Uint8Array>;
}

function hasPayloadKey(keys: Record<string, Uint8Array>, kid: string): boolean {
  return Object.prototype.hasOwnProperty.call(keys, kid);
}

function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let different = 0;
  for (let i = 0; i < a.length; i += 1) different |= a[i]! ^ b[i]!;
  return different === 0;
}

export type TreecrdtPayloadKeyringV1 = {
  activeKid: string;
  keys: Record<string, Uint8Array>;
};

function clonePayloadKeyringV1(keyring: TreecrdtPayloadKeyringV1): TreecrdtPayloadKeyringV1 {
  if (!keyring || typeof keyring !== 'object') throw new Error('keyring must be an object');

  const activeKid = assertPayloadKeyIdV1(keyring.activeKid, 'keyring.activeKid');
  const entries = Object.entries(keyring.keys ?? {});
  if (entries.length === 0) throw new Error('keyring.keys must not be empty');

  const keys = emptyPayloadKeys();
  for (const [rawKid, rawKey] of entries) {
    const kid = assertPayloadKeyIdV1(rawKid, 'keyring.keys[<kid>]');
    if (hasPayloadKey(keys, kid)) throw new Error(`duplicate keyring key id: ${kid}`);
    assertLen(rawKey, DOC_PAYLOAD_KEY_LEN, `keyring.keys[${kid}]`);
    keys[kid] = new Uint8Array(rawKey);
  }

  if (!hasPayloadKey(keys, activeKid)) {
    throw new Error('keyring.activeKid does not exist in keyring.keys');
  }

  return { activeKid, keys };
}

function payloadKeyFromKeyringV1(
  keyring: TreecrdtPayloadKeyringV1,
  kid: string,
): Uint8Array | null {
  if (!keyring || typeof keyring !== 'object') throw new Error('keyring must be an object');
  assertPayloadKeyIdV1(keyring.activeKid, 'keyring.activeKid');
  if (!keyring.keys || typeof keyring.keys !== 'object') {
    throw new Error('keyring.keys must be an object');
  }
  if (!hasPayloadKey(keyring.keys, kid)) return null;
  const payloadKey = keyring.keys[kid];
  if (!payloadKey) return null;
  assertLen(payloadKey, DOC_PAYLOAD_KEY_LEN, `keyring.keys[${kid}]`);
  return new Uint8Array(payloadKey);
}

function decodeEncryptedPayloadV1(bytes: Uint8Array): {
  nonce: Uint8Array;
  ct: Uint8Array;
  kid: string;
} {
  const map = assertMap(decodeCbor(bytes), 'EncryptedPayloadV1');
  const version = mapGet(map, 'v');
  if (version !== 1) throw new Error('EncryptedPayloadV1.v must be 1');

  const tag = assertString(mapGet(map, 't'), 'EncryptedPayloadV1.t');
  if (tag !== ENCRYPTED_PAYLOAD_V1_TAG) throw new Error('EncryptedPayloadV1.t mismatch');

  const alg = assertString(mapGet(map, 'alg'), 'EncryptedPayloadV1.alg');
  if (alg !== ENCRYPTED_PAYLOAD_ALG) throw new Error('EncryptedPayloadV1.alg unsupported');

  const kid = assertPayloadKeyIdV1(
    assertString(mapGet(map, 'kid'), 'EncryptedPayloadV1.kid'),
    'EncryptedPayloadV1.kid',
  );
  const nonce = assertLen(
    assertBytes(mapGet(map, 'nonce'), 'EncryptedPayloadV1.nonce'),
    AES_GCM_NONCE_LEN,
    'EncryptedPayloadV1.nonce',
  );
  const ct = assertBytes(mapGet(map, 'ct'), 'EncryptedPayloadV1.ct');
  return { nonce, ct, kid };
}

export function createTreecrdtPayloadKeyringV1(opts: {
  payloadKey: Uint8Array;
  activeKid?: string;
}): TreecrdtPayloadKeyringV1 {
  assertLen(opts.payloadKey, DOC_PAYLOAD_KEY_LEN, 'payloadKey');
  const activeKid = assertPayloadKeyIdV1(opts.activeKid ?? randomPayloadKeyIdV1(), 'activeKid');
  const keys = emptyPayloadKeys();
  keys[activeKid] = new Uint8Array(opts.payloadKey);
  return { activeKid, keys };
}

export function upsertTreecrdtPayloadKeyringKeyV1(opts: {
  keyring: TreecrdtPayloadKeyringV1;
  kid: string;
  payloadKey: Uint8Array;
  makeActive?: boolean;
}): TreecrdtPayloadKeyringV1 {
  const keyring = clonePayloadKeyringV1(opts.keyring);
  const kid = assertPayloadKeyIdV1(opts.kid, 'kid');
  assertLen(opts.payloadKey, DOC_PAYLOAD_KEY_LEN, 'payloadKey');

  if (hasPayloadKey(keyring.keys, kid)) {
    const existing = keyring.keys[kid];
    if (!existing || !equalBytes(existing, opts.payloadKey)) {
      throw new Error(`key id already exists with different key bytes: ${kid}`);
    }
  } else {
    keyring.keys[kid] = new Uint8Array(opts.payloadKey);
  }
  if (opts.makeActive ?? false) keyring.activeKid = kid;

  return keyring;
}

export function rotateTreecrdtPayloadKeyringV1(opts: {
  keyring: TreecrdtPayloadKeyringV1;
  nextKid?: string;
  nextPayloadKey?: Uint8Array;
}): {
  keyring: TreecrdtPayloadKeyringV1;
  rotatedKid: string;
  rotatedPayloadKey: Uint8Array;
} {
  const current = clonePayloadKeyringV1(opts.keyring);
  const rotatedKid = assertPayloadKeyIdV1(opts.nextKid ?? randomPayloadKeyIdV1(), 'nextKid');
  if (hasPayloadKey(current.keys, rotatedKid)) {
    throw new Error(`nextKid already exists in keyring: ${rotatedKid}`);
  }
  const rotatedPayloadKey = opts.nextPayloadKey
    ? new Uint8Array(opts.nextPayloadKey)
    : randomBytes(DOC_PAYLOAD_KEY_LEN);
  assertLen(rotatedPayloadKey, DOC_PAYLOAD_KEY_LEN, 'nextPayloadKey');

  const keyring = upsertTreecrdtPayloadKeyringKeyV1({
    keyring: current,
    kid: rotatedKid,
    payloadKey: rotatedPayloadKey,
    makeActive: true,
  });

  return { keyring, rotatedKid, rotatedPayloadKey };
}

export async function encryptTreecrdtPayloadWithKeyring(opts: {
  docId: string;
  keyring: TreecrdtPayloadKeyringV1;
  plaintext: Uint8Array;
}): Promise<Uint8Array> {
  const docId = assertDocId(opts.docId);
  if (!opts.keyring || typeof opts.keyring !== 'object') {
    throw new Error('keyring must be an object');
  }
  const activeKid = assertPayloadKeyIdV1(opts.keyring.activeKid, 'keyring.activeKid');
  const payloadKey = payloadKeyFromKeyringV1(opts.keyring, activeKid);
  if (!payloadKey) throw new Error('active payload key is missing');

  const nonce = randomBytes(AES_GCM_NONCE_LEN);
  const ciphertext = await aesGcmEncrypt({
    key: payloadKey,
    nonce,
    plaintext: opts.plaintext,
    aad: payloadAadV1(docId, activeKid),
  });

  const envelope = new Map<unknown, unknown>();
  envelope.set('v', 1);
  envelope.set('t', ENCRYPTED_PAYLOAD_V1_TAG);
  envelope.set('alg', ENCRYPTED_PAYLOAD_ALG);
  envelope.set('nonce', nonce);
  envelope.set('ct', ciphertext);
  envelope.set('kid', activeKid);
  return encodeCbor(envelope);
}

export async function decryptTreecrdtPayloadWithKeyring(opts: {
  docId: string;
  keyring: TreecrdtPayloadKeyringV1;
  ciphertext: Uint8Array;
}): Promise<Uint8Array> {
  const docId = assertDocId(opts.docId);
  const encrypted = decodeEncryptedPayloadV1(opts.ciphertext);
  const payloadKey = payloadKeyFromKeyringV1(opts.keyring, encrypted.kid);
  if (!payloadKey) throw new Error(`payload key not found: ${encrypted.kid}`);

  return await aesGcmDecrypt({
    key: payloadKey,
    nonce: encrypted.nonce,
    ciphertext: encrypted.ct,
    aad: payloadAadV1(docId, encrypted.kid),
  });
}
