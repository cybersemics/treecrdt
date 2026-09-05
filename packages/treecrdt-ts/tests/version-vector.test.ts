import { runInNewContext } from 'node:vm';

import { describe, expect, test } from 'vitest';

import {
  decodeVersionVectorV0,
  encodeVersionVectorV0,
  VersionVectorV0CodecError,
  type VersionVectorEntryV0,
  type VersionVectorRangeV0,
  type VersionVectorV0,
} from '@treecrdt/interface/version-vector';
import fixture from '../../../fixtures/version-vector-v0.json';

const MAX_U64 = (1n << 64n) - 1n;

function fromHex(value: string): Uint8Array {
  return Uint8Array.from(value.match(/../g)!.map((byte) => Number.parseInt(byte, 16)));
}

function entry(
  replica: number[] = [1],
  frontier = 1n,
  ranges: readonly VersionVectorRangeV0[] = [],
): VersionVectorEntryV0 {
  return { replica: Uint8Array.from(replica), frontier, ranges };
}

function uncheckedVersionVector(value: unknown): VersionVectorV0 {
  return value as VersionVectorV0;
}

describe('VersionVectorV0 codec', () => {
  test('has a stable encoding for the empty vector', () => {
    const encoded = encodeVersionVectorV0({ entries: [] });
    expect(encoded).toEqual(fromHex(fixture.validEncodedHex.empty));
    expect(decodeVersionVectorV0(encoded)).toEqual({ entries: [] });
  });

  test('matches the cross-runtime vector for prefix ordering, gaps, and the maximum counter', () => {
    const vector: VersionVectorV0 = {
      entries: [
        entry([0], 2n, [
          [4n, 5n],
          [MAX_U64, MAX_U64],
        ]),
        entry([0, 1], 1n, [[3n, 3n]]),
      ],
    };
    const expected = fromHex(fixture.validEncodedHex.prefixOrderGapsAndMaxCounter);

    expect(encodeVersionVectorV0(vector)).toEqual(expected);
    expect(decodeVersionVectorV0(expected)).toEqual(vector);
  });

  test('decodes a view with a non-zero byte offset and copies replica bytes', () => {
    const encoded = encodeVersionVectorV0({ entries: [entry([7, 8, 9])] });
    const wrapped = new Uint8Array(encoded.length + 4);
    wrapped.set(encoded, 2);
    const view = wrapped.subarray(2, 2 + encoded.length);
    const decoded = decodeVersionVectorV0(view);

    view.fill(0);
    expect(decoded.entries[0]!.replica).toEqual(Uint8Array.of(7, 8, 9));
  });

  test('accepts Uint8Array values from another realm', () => {
    const replica = runInNewContext('Uint8Array.of(7, 8, 9)') as Uint8Array;
    const encoded = encodeVersionVectorV0({
      entries: [{ replica, frontier: 1n, ranges: [] }],
    });
    const foreignEncoded = runInNewContext('Uint8Array.from(values)', {
      values: Array.from(encoded),
    }) as Uint8Array;

    expect(replica instanceof Uint8Array).toBe(false);
    expect(foreignEncoded instanceof Uint8Array).toBe(false);
    const decodedReplica = decodeVersionVectorV0(foreignEncoded).entries[0]!.replica;
    expect(decodedReplica).toEqual(Uint8Array.of(7, 8, 9));
    expect(decodedReplica instanceof Uint8Array).toBe(true);
  });

  test('encoder accepts zero-length replica IDs and rejects invalid replica values or ordering', () => {
    const zeroLengthReplica = { entries: [entry([])] };
    expect(decodeVersionVectorV0(encodeVersionVectorV0(zeroLengthReplica))).toEqual(
      zeroLengthReplica,
    );
    expect(() => encodeVersionVectorV0({ entries: [entry([2]), entry([1])] })).toThrow(
      /strictly sorted/i,
    );
    expect(() => encodeVersionVectorV0({ entries: [entry([1]), entry([1])] })).toThrow(
      /strictly sorted/i,
    );
    expect(() =>
      encodeVersionVectorV0(
        uncheckedVersionVector({ entries: [{ replica: [1], frontier: 1n, ranges: [] }] }),
      ),
    ).toThrow(/Uint8Array/i);
  });

  test('encoder rejects semantically empty entries and invalid counters', () => {
    expect(() => encodeVersionVectorV0({ entries: [entry([1], 0n)] })).toThrow(
      /semantically empty/i,
    );

    for (const frontier of [-1n, MAX_U64 + 1n, 1] as unknown[]) {
      expect(() =>
        encodeVersionVectorV0(
          uncheckedVersionVector({
            entries: [{ replica: Uint8Array.of(1), frontier, ranges: [] }],
          }),
        ),
      ).toThrow(/u64 range/i);
    }
  });

  test.each<[string, bigint, readonly (VersionVectorRangeV0 | readonly bigint[])[]]>([
    ['zero start', 0n, [[0n, 1n]]],
    ['reversed bounds', 0n, [[3n, 2n]]],
    ['frontier overlap', 2n, [[2n, 4n]]],
    ['frontier adjacency', 2n, [[3n, 4n]]],
    [
      'unsorted ranges',
      0n,
      [
        [5n, 6n],
        [3n, 3n],
      ],
    ],
    [
      'overlapping ranges',
      0n,
      [
        [3n, 5n],
        [5n, 7n],
      ],
    ],
    [
      'adjacent ranges',
      0n,
      [
        [3n, 4n],
        [5n, 6n],
      ],
    ],
    ['out-of-range end', 0n, [[2n, MAX_U64 + 1n]]],
  ])('encoder rejects %s', (_name, frontier, ranges) => {
    expect(() =>
      encodeVersionVectorV0(
        uncheckedVersionVector({ entries: [{ replica: Uint8Array.of(1), frontier, ranges }] }),
      ),
    ).toThrow(VersionVectorV0CodecError);
  });

  test('encoder requires two bigint bounds per range', () => {
    for (const range of [[2n], [2n, 3n, 4n], [2, 3]]) {
      expect(() =>
        encodeVersionVectorV0(
          uncheckedVersionVector({
            entries: [{ replica: Uint8Array.of(1), frontier: 0n, ranges: [range] }],
          }),
        ),
      ).toThrow(VersionVectorV0CodecError);
    }
  });

  test.each(Object.entries(fixture.invalidEncodedHex))(
    'decoder rejects shared invalid encoding %s',
    (_name, encodedHex) => {
      expect(() => decodeVersionVectorV0(fromHex(encodedHex))).toThrow(VersionVectorV0CodecError);
    },
  );

  test('decoder rejects every truncation of a canonical value', () => {
    const canonical = encodeVersionVectorV0({ entries: [entry([1, 2], 1n, [[3n, 4n]])] });
    for (let length = 0; length < canonical.length; length += 1) {
      expect(() => decodeVersionVectorV0(canonical.slice(0, length))).toThrow(
        VersionVectorV0CodecError,
      );
    }
  });
});
