import { runInNewContext } from 'node:vm';

import { describe, expect, test } from 'vitest';

import {
  decodeVersionVectorV0,
  encodeVersionVectorV0,
  VersionVectorCodecError,
  type VersionVectorEntry,
  type VersionVectorRange,
  type VersionVector,
} from '@treecrdt/wasm/codec';
import fixture from '../../../fixtures/version-vector-v0.json';
import { createVersionVectorCodec } from '../src/codec.js';

const MAX_U64 = (1n << 64n) - 1n;

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
  test('shares lazy initialization and retries loader failures without treating them as invalid data', async () => {
    const unavailable = new Error('WASM fetch unavailable');
    let attempts = 0;
    const codec = createVersionVectorCodec(async () => {
      attempts += 1;
      if (attempts === 1) throw unavailable;
      return import('../pkg/treecrdt_wasm.js');
    });
    expect(attempts).toBe(0);
    const failed = [
      codec.encodeVersionVectorV0({ entries: [] }),
      codec.encodeVersionVectorV0({ entries: [] }),
    ];
    await Promise.all(failed.map((pending) => expect(pending).rejects.toBe(unavailable)));
    expect(attempts).toBe(1);
    const expected = fromHex(fixture.validEncodedHex.empty);
    await expect(codec.encodeVersionVectorV0({ entries: [] })).resolves.toEqual(expected);
    await expect(codec.encodeVersionVectorV0({ entries: [] })).resolves.toEqual(expected);
    expect(attempts).toBe(2);
  });

  test.each([new WebAssembly.RuntimeError('unreachable'), new TypeError('bridge failure')])(
    'preserves unexpected WASM failures: %s',
    async (failure) => {
      const fail = () => {
        throw failure;
      };
      const codec = createVersionVectorCodec(async () => ({
        encodeVersionVectorV0: fail,
        decodeVersionVectorV0: fail,
      }));
      await expect(codec.encodeVersionVectorV0({ entries: [] })).rejects.toBe(failure);
      await expect(codec.decodeVersionVectorV0(new Uint8Array())).rejects.toBe(failure);
    },
  );

  test('does not classify invalid bridge output as invalid caller input', async () => {
    const wasm = await import('../pkg/treecrdt_wasm.js');
    const codec = createVersionVectorCodec(async () => ({
      ...wasm,
      decodeVersionVectorV0: () => ({ entries: null! }),
    }));
    await expect(
      codec.decodeVersionVectorV0(fromHex(fixture.validEncodedHex.empty)),
    ).rejects.toThrow(TypeError);
  });

  test('matches the shared empty-vector encoding', async () => {
    const encoded = await encodeVersionVectorV0({ entries: [] });
    expect(encoded).toEqual(fromHex(fixture.validEncodedHex.empty));
    expect(await decodeVersionVectorV0(encoded)).toEqual({ entries: [] });
  });

  test('preserves prefix ordering, gaps, and full-width bigint counters', async () => {
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
    expect(await encodeVersionVectorV0(vector)).toEqual(expected);
    expect(await decodeVersionVectorV0(expected)).toEqual(vector);
  });

  test('handles byte offsets and snapshots inputs before yielding', async () => {
    const replica = Uint8Array.of(0, 7, 8, 9, 0).subarray(1, 4);
    const ranges: [bigint, bigint][] = [[3n, 4n]];
    const entries = [{ replica, frontier: 1n, ranges }];
    const encodedPromise = encodeVersionVectorV0({ entries });
    replica.fill(0);
    ranges[0]![1] = 99n;
    entries.length = 0;
    const encoded = await encodedPromise;

    const wrapped = new Uint8Array(encoded.length + 4);
    wrapped.set(encoded, 2);
    const view = wrapped.subarray(2, 2 + encoded.length);
    const decodedPromise = decodeVersionVectorV0(view);
    view.fill(0);
    const decoded = await decodedPromise;
    expect(decoded).toEqual({ entries: [entry([7, 8, 9], 1n, [[3n, 4n]])] });
    expect(decoded.entries[0]!.replica.buffer).not.toBe(wrapped.buffer);
  });

  test('accepts Uint8Array values from another realm', async () => {
    const replica = runInNewContext('Uint8Array.of(7, 8, 9)') as Uint8Array;
    const encoded = await encodeVersionVectorV0({
      entries: [{ replica, frontier: 1n, ranges: [] }],
    });
    const foreignEncoded = runInNewContext('Uint8Array.from(values)', {
      values: Array.from(encoded),
    }) as Uint8Array;

    expect(replica instanceof Uint8Array).toBe(false);
    expect(foreignEncoded instanceof Uint8Array).toBe(false);
    const decoded = await decodeVersionVectorV0(foreignEncoded);
    expect(decoded.entries[0]!.replica).toEqual(Uint8Array.of(7, 8, 9));
    expect(decoded.entries[0]!.replica instanceof Uint8Array).toBe(true);
  });

  test('supports zero-length replica IDs', async () => {
    const vector = { entries: [entry([])] };
    expect(await decodeVersionVectorV0(await encodeVersionVectorV0(vector))).toEqual(vector);
  });

  test.each([
    ['unsorted replicas', [entry([2]), entry([1])]],
    ['duplicate replicas', [entry([1]), entry([1])]],
    ['empty entry', [entry([1], 0n)]],
    ['negative counter', [entry([1], -1n)]],
    ['counter overflow', [entry([1], MAX_U64 + 1n)]],
    ['non-normalized range', [entry([1], 2n, [[3n, 4n]])]],
    ['range overflow', [entry([1], 0n, [[2n, MAX_U64 + 1n]])]],
  ])('forwards Rust encoder rejection of %s', async (_name, entries) => {
    await expect(encodeVersionVectorV0({ entries } as VersionVector)).rejects.toThrow(
      VersionVectorCodecError,
    );
  });

  test.each([
    ['non-object vector', null],
    ['non-array entries', { entries: new Set() }],
    ['non-object entry', { entries: [null] }],
    ['plain-array replica', { entries: [{ ...entry(), replica: [1] }] }],
    ['numeric counter', { entries: [{ ...entry(), frontier: 1 }] }],
    ['non-array ranges', { entries: [{ ...entry(), ranges: new Set() }] }],
    ['short range', { entries: [{ ...entry(), ranges: [[2n]] }] }],
    ['long range', { entries: [{ ...entry(), ranges: [[2n, 3n, 4n]] }] }],
    ['numeric range', { entries: [{ ...entry(), ranges: [[2, 3]] }] }],
  ])('rejects JS input types that WASM would otherwise coerce: %s', async (_name, vector) => {
    await expect(encodeVersionVectorV0(vector as VersionVector)).rejects.toThrow(
      VersionVectorCodecError,
    );
  });

  test.each([[], new Uint16Array(4), new DataView(new ArrayBuffer(8)), null])(
    'rejects decode input that is not a Uint8Array',
    async (value) => {
      await expect(decodeVersionVectorV0(value as Uint8Array)).rejects.toThrow(
        VersionVectorCodecError,
      );
    },
  );

  test.each(Object.entries(fixture.invalidEncodedHex))(
    'rejects shared invalid encoding %s',
    async (_name, encodedHex) => {
      await expect(decodeVersionVectorV0(fromHex(encodedHex))).rejects.toThrow(
        VersionVectorCodecError,
      );
    },
  );

  test('rejects every truncation of a canonical value', async () => {
    const canonical = await encodeVersionVectorV0({ entries: [entry([1, 2], 1n, [[3n, 4n]])] });
    for (let length = 0; length < canonical.length; length += 1) {
      await expect(decodeVersionVectorV0(canonical.slice(0, length))).rejects.toThrow(
        VersionVectorCodecError,
      );
    }
  });
});
