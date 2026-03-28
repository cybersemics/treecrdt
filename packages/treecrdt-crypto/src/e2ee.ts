import { utf8ToBytes } from '@noble/hashes/utils';

import {
  aesGcmDecrypt,
  aesGcmEncrypt,
  assertBytes,
  assertLen,
  assertMap,
  assertString,
  concatBytes,
  decodeCbor,
  encodeCbor,
  mapGet,
  randomBytes,
} from './internal/util.js';

const DOC_PAYLOAD_KEY_LEN = 32;
const AES_GCM_NONCE_LEN = 12;

const ENCRYPTED_PAYLOAD_V1_TAG = 'treecrdt/payload-encrypted/v1';
const ENCRYPTED_PAYLOAD_V1_AAD_DOMAIN = utf8ToBytes('treecrdt/payload-encrypted/v1');

function payloadAadV1(docId: string): Uint8Array {
  return concatBytes(ENCRYPTED_PAYLOAD_V1_AAD_DOMAIN, utf8ToBytes(docId));
}

function tryDecodeEncryptedPayloadV1(
  bytes: Uint8Array,
): { nonce: Uint8Array; ct: Uint8Array } | null {
  let decoded: unknown;
  try {
    decoded = decodeCbor(bytes);
  } catch {
    return null;
  }
  if (!(decoded instanceof Map)) return null;

  const map = decoded as Map<unknown, unknown>;
  const v = mapGet(map, 'v');
  if (v !== 1) return null;
  const t = mapGet(map, 't');
  if (t !== ENCRYPTED_PAYLOAD_V1_TAG) return null;

  const alg = mapGet(map, 'alg');
  if (alg !== 'A256GCM') return null;

  const nonce = mapGet(map, 'nonce');
  if (!(nonce instanceof Uint8Array)) return null;
  if (nonce.length !== AES_GCM_NONCE_LEN) return null;

  const ct = mapGet(map, 'ct');
  if (!(ct instanceof Uint8Array)) return null;

  return { nonce, ct };
}

export function isTreecrdtEncryptedPayloadV1(bytes: Uint8Array): boolean {
  return tryDecodeEncryptedPayloadV1(bytes) !== null;
}

export async function encryptTreecrdtPayloadV1(opts: {
  docId: string;
  payloadKey: Uint8Array;
  plaintext: Uint8Array;
}): Promise<Uint8Array> {
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error('docId must not be empty');
  assertLen(opts.payloadKey, DOC_PAYLOAD_KEY_LEN, 'payloadKey');

  const nonce = randomBytes(AES_GCM_NONCE_LEN);
  const ciphertext = await aesGcmEncrypt({
    key: opts.payloadKey,
    nonce,
    plaintext: opts.plaintext,
    aad: payloadAadV1(opts.docId),
  });

  const envelope = new Map<unknown, unknown>();
  envelope.set('v', 1);
  envelope.set('t', ENCRYPTED_PAYLOAD_V1_TAG);
  envelope.set('alg', 'A256GCM');
  envelope.set('nonce', nonce);
  envelope.set('ct', ciphertext);
  return encodeCbor(envelope);
}

export async function decryptTreecrdtPayloadV1(opts: {
  docId: string;
  payloadKey: Uint8Array;
  ciphertext: Uint8Array;
}): Promise<Uint8Array> {
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error('docId must not be empty');
  assertLen(opts.payloadKey, DOC_PAYLOAD_KEY_LEN, 'payloadKey');

  const decoded = decodeCbor(opts.ciphertext);
  const map = assertMap(decoded, 'EncryptedPayloadV1');

  const v = mapGet(map, 'v');
  if (v !== 1) throw new Error('EncryptedPayloadV1.v must be 1');
  const t = assertString(mapGet(map, 't'), 'EncryptedPayloadV1.t');
  if (t !== ENCRYPTED_PAYLOAD_V1_TAG) throw new Error('EncryptedPayloadV1.t mismatch');
  const alg = assertString(mapGet(map, 'alg'), 'EncryptedPayloadV1.alg');
  if (alg !== 'A256GCM') throw new Error('EncryptedPayloadV1.alg unsupported');

  const nonce = assertLen(
    assertBytes(mapGet(map, 'nonce'), 'EncryptedPayloadV1.nonce'),
    AES_GCM_NONCE_LEN,
    'nonce',
  );
  const ct = assertBytes(mapGet(map, 'ct'), 'EncryptedPayloadV1.ct');

  return await aesGcmDecrypt({
    key: opts.payloadKey,
    nonce,
    ciphertext: ct,
    aad: payloadAadV1(opts.docId),
  });
}

export async function maybeDecryptTreecrdtPayloadV1(opts: {
  docId: string;
  payloadKey: Uint8Array;
  bytes: Uint8Array;
}): Promise<{ plaintext: Uint8Array; encrypted: boolean }> {
  const parsed = tryDecodeEncryptedPayloadV1(opts.bytes);
  if (!parsed) return { plaintext: opts.bytes, encrypted: false };

  const plaintext = await aesGcmDecrypt({
    key: opts.payloadKey,
    nonce: parsed.nonce,
    ciphertext: parsed.ct,
    aad: payloadAadV1(opts.docId),
  });

  return { plaintext, encrypted: true };
}
