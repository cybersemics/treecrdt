import { decode as cborDecode, encode as cborEncode, rfc8949EncodeOptions } from 'cborg';

export function encodeCbor(value: unknown): Uint8Array {
  return cborEncode(value, rfc8949EncodeOptions);
}

export function decodeCbor(bytes: Uint8Array): unknown {
  return cborDecode(bytes, { useMaps: true });
}

export function concatBytes(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

export function assertBytes(val: unknown, field: string): Uint8Array {
  if (!(val instanceof Uint8Array)) throw new Error(`${field} must be bytes`);
  return val;
}

export function assertString(val: unknown, field: string): string {
  if (typeof val !== 'string') throw new Error(`${field} must be a string`);
  return val;
}

export function assertLen(bytes: Uint8Array, expected: number, field: string): Uint8Array {
  if (bytes.length !== expected) {
    throw new Error(`${field}: expected ${expected} bytes, got ${bytes.length}`);
  }
  return bytes;
}

export function assertMap(val: unknown, ctx: string): Map<unknown, unknown> {
  if (!(val instanceof Map)) throw new Error(`${ctx} must be a CBOR map`);
  return val;
}

export function mapGet(map: Map<unknown, unknown>, key: unknown): unknown {
  return map.has(key) ? map.get(key) : undefined;
}

export function randomBytes(len: number): Uint8Array {
  const cryptoObj = (globalThis as any).crypto as
    | { getRandomValues?: (arr: Uint8Array) => Uint8Array }
    | undefined;
  if (!cryptoObj?.getRandomValues) throw new Error('crypto.getRandomValues is not available');
  const bytes = new Uint8Array(len);
  cryptoObj.getRandomValues(bytes);
  return bytes;
}

export async function aesGcmEncrypt(opts: {
  key: Uint8Array;
  nonce: Uint8Array;
  plaintext: Uint8Array;
  aad?: Uint8Array;
}): Promise<Uint8Array> {
  const cryptoObj = (globalThis as any).crypto as { subtle?: any } | undefined;
  if (!cryptoObj?.subtle) throw new Error('crypto.subtle is not available');

  const key = await cryptoObj.subtle.importKey('raw', opts.key, { name: 'AES-GCM' }, false, [
    'encrypt',
  ]);
  const ciphertext = await cryptoObj.subtle.encrypt(
    {
      name: 'AES-GCM',
      iv: opts.nonce,
      ...(opts.aad ? { additionalData: opts.aad } : {}),
      tagLength: 128,
    },
    key,
    opts.plaintext,
  );
  return new Uint8Array(ciphertext);
}

export async function aesGcmDecrypt(opts: {
  key: Uint8Array;
  nonce: Uint8Array;
  ciphertext: Uint8Array;
  aad?: Uint8Array;
}): Promise<Uint8Array> {
  const cryptoObj = (globalThis as any).crypto as { subtle?: any } | undefined;
  if (!cryptoObj?.subtle) throw new Error('crypto.subtle is not available');

  const key = await cryptoObj.subtle.importKey('raw', opts.key, { name: 'AES-GCM' }, false, [
    'decrypt',
  ]);
  const plaintext = await cryptoObj.subtle.decrypt(
    {
      name: 'AES-GCM',
      iv: opts.nonce,
      ...(opts.aad ? { additionalData: opts.aad } : {}),
      tagLength: 128,
    },
    key,
    opts.ciphertext,
  );
  return new Uint8Array(plaintext);
}
