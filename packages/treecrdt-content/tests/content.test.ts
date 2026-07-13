import { describe, expect, test } from 'vitest';

import { decodeContent, encodeImageContent, encodeTextContent } from '../src/index.js';

function hexToBytes(hex: string): Uint8Array {
  if (!/^(?:[0-9a-f]{2})+$/i.test(hex)) throw new Error('invalid test fixture hex');
  return Uint8Array.from(hex.match(/.{2}/g)!.map((byte) => Number.parseInt(byte, 16)));
}

describe('@treecrdt/content', () => {
  test('keeps text as raw UTF-8 and treats null as empty content', () => {
    const bytes = encodeTextContent('hello image world');

    expect(decodeContent(bytes)).toMatchObject({
      kind: 'text',
      text: 'hello image world',
    });
    expect(decodeContent(null)).toEqual({ kind: 'empty' });
  });

  test('roundtrips an image envelope', () => {
    const encoded = encodeImageContent({
      mime: 'image/png',
      name: ' pixel.png ',
      bytes: new Uint8Array([1, 2, 3, 4]),
    });

    const decoded = decodeContent(encoded);
    expect(decoded).toMatchObject({
      kind: 'image',
      mime: 'image/png',
      name: 'pixel.png',
      size: 4,
    });
    expect(decoded.kind === 'image' ? Array.from(decoded.bytes) : []).toEqual([1, 2, 3, 4]);
  });

  test('locks the v1 wire format with a static fixture', () => {
    const fixture = hexToBytes(
      '89544352434e54010000003f7b226b696e64223a22696d616765222c226d696d65223a22696d6167652f706e67222c2273697a65223a332c226e616d65223a22706978656c2e706e67227d010203',
    );

    expect(decodeContent(fixture)).toMatchObject({
      kind: 'image',
      mime: 'image/png',
      name: 'pixel.png',
      size: 3,
    });
    expect(
      encodeImageContent({
        mime: 'image/png',
        name: 'pixel.png',
        bytes: new Uint8Array([1, 2, 3]),
      }),
    ).toEqual(fixture);
  });

  test('handles Uint8Array views with non-zero offsets', () => {
    const encoded = encodeImageContent({
      mime: 'image/webp',
      bytes: new Uint8Array([9, 8, 7]),
    });
    const padded = new Uint8Array(encoded.byteLength + 4);
    padded.set(encoded, 2);

    expect(decodeContent(padded.subarray(2, -2))).toMatchObject({
      kind: 'image',
      mime: 'image/webp',
      size: 3,
    });
  });

  test('rejects unsupported image MIME types', () => {
    expect(() =>
      encodeImageContent({
        mime: 'image/svg+xml',
        bytes: new Uint8Array([1]),
      }),
    ).toThrow(/Unsupported image content MIME type/);
  });

  test('fails closed for malformed or unsupported recognized envelopes', () => {
    const encoded = encodeImageContent({
      mime: 'image/png',
      bytes: new Uint8Array([1]),
    });

    const unsupportedVersion = encoded.slice();
    unsupportedVersion[7] = 2;
    expect(() => decodeContent(unsupportedVersion)).toThrow(/unsupported.*version/i);
    expect(() => decodeContent(encoded.subarray(0, 10))).toThrow(/truncated/i);

    const invalidMetadataLength = encoded.slice();
    invalidMetadataLength.fill(0, 8, 12);
    expect(() => decodeContent(invalidMetadataLength)).toThrow(/metadata length/i);

    expect(() => decodeContent(encoded.subarray(0, -1))).toThrow(/size mismatch/i);
    const appended = new Uint8Array(encoded.byteLength + 1);
    appended.set(encoded);
    expect(() => decodeContent(appended)).toThrow(/size mismatch/i);
  });
});
