import { runInNewContext } from 'node:vm';

import { beforeAll, describe, expect, test } from 'vitest';

import {
  loadVersionVectorCodec,
  VersionVectorCodecError,
  type VersionVectorCodec,
  type VersionVectorEntry,
  type VersionVectorRange,
  type VersionVector,
} from '@treecrdt/wasm/codec';
import fixture from '../../../fixtures/version-vector-v0.json';
import { createVersionVectorCodecLoader } from '../src/codec.js';

const MAX_U64 = (1n << 64n) - 1n;
let codec: VersionVectorCodec;

beforeAll(async () => {
  codec = await loadVersionVectorCodec();
});

function fromHex(value: string): Uint8Array {
  return Uint8Array.from(value.match(/../g)!.map((byte) => Number.parseInt(byte, 16)));
}

function entry(
  replica: number[] = [1],
  frontier = 1n,
  ranges: readonly VersionVectorRange[] = [],
): VersionVectorEntry {
  return { replica: Uint8Array.from(replica), frontier, ranges };
}

describe('Rust VersionVectorV0 codec through WASM', () => {
  test('initializes lazily and shares one ready codec across concurrent and later loads', async () => {
    let attempts = 0;
    const loadCodec = createVersionVectorCodecLoader(async () => {
      attempts += 1;
      return import('../pkg/treecrdt_wasm.js');
    });
    expect(attempts).toBe(0);
    const [first, second] = await Promise.all([loadCodec(), loadCodec()]);
    expect(second).toBe(first);
    expect(await loadCodec()).toBe(first);
    expect(attempts).toBe(1);

    const expected = fromHex(fixture.validEncodedHex.empty);
    expect(first.encodeVersionVectorV0({ entries: [] })).toEqual(expected);
    expect(first.decodeVersionVectorV0(expected)).toEqual({ entries: [] });
    expect(attempts).toBe(1);
  });

  test('retries loader failures without treating them as invalid data', async () => {
    const unavailable = new Error('WASM fetch unavailable');
    let attempts = 0;
    const loadCodec = createVersionVectorCodecLoader(async () => {
      attempts += 1;
      if (attempts === 1) throw unavailable;
      return import('../pkg/treecrdt_wasm.js');
    });
    expect(attempts).toBe(0);
    const failed = [loadCodec(), loadCodec()];
    await Promise.all(failed.map((pending) => expect(pending).rejects.toBe(unavailable)));
    expect(attempts).toBe(1);
    const readyCodec = await loadCodec();
    expect(await loadCodec()).toBe(readyCodec);
    expect(attempts).toBe(2);
  });

  test('passes inputs directly to synchronous WASM methods', async () => {
    const vector = { entries: [entry([7, 8, 9], 1n, [[3n, 4n]])] };
    const bytes = fromHex(fixture.validEncodedHex.empty);
    const loadCodec = createVersionVectorCodecLoader(async () => ({
      encodeVersionVectorV0(input) {
        expect(input).toBe(vector);
        return bytes;
      },
      decodeVersionVectorV0(input) {
        expect(input).toBe(bytes);
        return vector;
      },
    }));
    const readyCodec = await loadCodec();
    expect(readyCodec.encodeVersionVectorV0(vector)).toBe(bytes);
    expect(readyCodec.decodeVersionVectorV0(bytes)).toBe(vector);
  });

  test.each([new WebAssembly.RuntimeError('unreachable'), new TypeError('bridge failure')])(
    'preserves unexpected WASM failures: %s',
    async (failure) => {
      const fail = () => {
        throw failure;
      };
      const loadCodec = createVersionVectorCodecLoader(async () => ({
        encodeVersionVectorV0: fail,
        decodeVersionVectorV0: fail,
      }));
      const readyCodec = await loadCodec();
      for (const call of [
        () => readyCodec.encodeVersionVectorV0({ entries: [] }),
        () => readyCodec.decodeVersionVectorV0(new Uint8Array()),
      ]) {
        let thrown;
        try {
          call();
        } catch (error) {
          thrown = error;
        }
        expect(thrown).toBe(failure);
      }
    },
  );

  test('matches the shared empty-vector encoding', () => {
    const encoded = codec.encodeVersionVectorV0({ entries: [] });
    expect(encoded).toEqual(fromHex(fixture.validEncodedHex.empty));
    expect(codec.decodeVersionVectorV0(encoded)).toEqual({ entries: [] });
  });

  test('preserves prefix ordering, gaps, and full-width bigint counters', () => {
    const vector: VersionVector = {
      entries: [
        entry([0], 2n, [
          [4n, 5n],
          [MAX_U64, MAX_U64],
        ]),
        entry([0, 1], 1n, [[3n, 3n]]),
      ],
    };
    const expected = fromHex(fixture.validEncodedHex.prefixOrderGapsAndMaxCounter);
    expect(codec.encodeVersionVectorV0(vector)).toEqual(expected);
    expect(codec.decodeVersionVectorV0(expected)).toEqual(vector);
  });

  test('handles byte offsets and completes before callers can mutate inputs', () => {
    const replica = Uint8Array.of(0, 7, 8, 9, 0).subarray(1, 4);
    const ranges: [bigint, bigint][] = [[3n, 4n]];
    const entries = [{ replica, frontier: 1n, ranges }];
    const encoded = codec.encodeVersionVectorV0({ entries });
    replica.fill(0);
    ranges[0]![1] = 99n;
    entries.length = 0;

    const wrapped = new Uint8Array(encoded.length + 4);
    wrapped.set(encoded, 2);
    const view = wrapped.subarray(2, 2 + encoded.length);
    const decoded = codec.decodeVersionVectorV0(view);
    view.fill(0);
    expect(decoded).toEqual({ entries: [entry([7, 8, 9], 1n, [[3n, 4n]])] });
    expect(decoded.entries[0]!.replica.buffer).not.toBe(wrapped.buffer);
  });

  test('accepts Uint8Array values from another realm', () => {
    const replica = runInNewContext('Uint8Array.of(7, 8, 9)') as Uint8Array;
    const encoded = codec.encodeVersionVectorV0({
      entries: [{ replica, frontier: 1n, ranges: [] }],
    });
    const foreignEncoded = runInNewContext('Uint8Array.from(values)', {
      values: Array.from(encoded),
    }) as Uint8Array;

    expect(replica instanceof Uint8Array).toBe(false);
    expect(foreignEncoded instanceof Uint8Array).toBe(false);
    const decoded = codec.decodeVersionVectorV0(foreignEncoded);
    expect(decoded.entries[0]!.replica).toEqual(Uint8Array.of(7, 8, 9));
    expect(decoded.entries[0]!.replica instanceof Uint8Array).toBe(true);
  });

  test('supports zero-length replica IDs', () => {
    const vector = { entries: [entry([])] };
    expect(codec.decodeVersionVectorV0(codec.encodeVersionVectorV0(vector))).toEqual(vector);
  });

  test('accepts safe numeric counters, byte arrays, and iterables through Serde', () => {
    const vector = {
      entries: new Set([
        { replica: [1], frontier: 1, ranges: new Set([new Set([3, Number.MAX_SAFE_INTEGER])]) },
      ]),
    };
    const encoded = codec.encodeVersionVectorV0(vector as unknown as VersionVector);
    expect(codec.decodeVersionVectorV0(encoded)).toEqual({
      entries: [entry([1], 1n, [[3n, BigInt(Number.MAX_SAFE_INTEGER)]])],
    });
  });

  test.each([
    ['unsorted replicas', [entry([2]), entry([1])]],
    ['duplicate replicas', [entry([1]), entry([1])]],
    ['empty entry', [entry([1], 0n)]],
    ['negative counter', [entry([1], -1n)]],
    ['counter overflow', [entry([1], MAX_U64 + 1n)]],
    ['non-normalized range', [entry([1], 2n, [[3n, 4n]])]],
    ['range overflow', [entry([1], 0n, [[2n, MAX_U64 + 1n]])]],
  ])('forwards Rust encoder rejection of %s', (_name, entries) => {
    expect(() => codec.encodeVersionVectorV0({ entries } as VersionVector)).toThrow(
      VersionVectorCodecError,
    );
  });

  test.each([
    ['non-object vector', null],
    ['non-iterable entries', { entries: 1 }],
    ['non-object entry', { entries: [null] }],
    ['invalid replica byte', { entries: [{ ...entry(), replica: [256] }] }],
    [
      'unsafe numeric counter',
      { entries: [{ ...entry(), frontier: Number.MAX_SAFE_INTEGER + 1 }] },
    ],
    ['non-iterable ranges', { entries: [{ ...entry(), ranges: 1 }] }],
    ['short range', { entries: [{ ...entry(), ranges: [[2n]] }] }],
    ['long range', { entries: [{ ...entry(), ranges: [[3n, 4n, 5n]] }] }],
    ['long iterable range', { entries: [{ ...entry(), ranges: [new Set([3n, 4n, 5n])] }] }],
  ])('rejects malformed or unsafe JS input: %s', (_name, vector) => {
    expect(() => codec.encodeVersionVectorV0(vector as VersionVector)).toThrow(
      VersionVectorCodecError,
    );
  });

  test.each([[], new Uint16Array(4), new DataView(new ArrayBuffer(8)), null])(
    'rejects decode input that is not a Uint8Array',
    (value) => {
      expect(() => codec.decodeVersionVectorV0(value as Uint8Array)).toThrow(
        VersionVectorCodecError,
      );
    },
  );

  test.each(Object.entries(fixture.invalidEncodedHex))(
    'rejects shared invalid encoding %s',
    (_name, encodedHex) => {
      expect(() => codec.decodeVersionVectorV0(fromHex(encodedHex))).toThrow(
        VersionVectorCodecError,
      );
    },
  );

  test('rejects every truncation of a canonical value', () => {
    const canonical = codec.encodeVersionVectorV0({ entries: [entry([1, 2], 1n, [[3n, 4n]])] });
    for (let length = 0; length < canonical.length; length += 1) {
      expect(() => codec.decodeVersionVectorV0(canonical.slice(0, length))).toThrow(
        VersionVectorCodecError,
      );
    }
  });
});
