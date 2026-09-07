/** Inclusive counter bounds for observations beyond the contiguous frontier. */
export type VersionVectorRange = readonly [start: bigint, end: bigint];

export type VersionVectorEntry = {
  readonly replica: Uint8Array;
  /** Highest counter for which every counter from 1 through it has been observed. */
  readonly frontier: bigint;
  readonly ranges: readonly VersionVectorRange[];
};

export type VersionVector = {
  readonly entries: readonly VersionVectorEntry[];
};

export class VersionVectorCodecError extends Error {
  constructor(message: string) {
    super(`invalid VersionVectorV0: ${message}`);
    this.name = 'VersionVectorCodecError';
  }
}

export type VersionVectorCodec = {
  encodeVersionVectorV0(vector: VersionVector): Uint8Array;
  decodeVersionVectorV0(bytes: Uint8Array): VersionVector;
};

function invalid(message: string): never {
  throw new VersionVectorCodecError(message);
}

function assertBytes(value: unknown): asserts value is Uint8Array {
  if (
    !ArrayBuffer.isView(value) ||
    Object.prototype.toString.call(value) !== '[object Uint8Array]'
  ) {
    invalid('bytes must be a Uint8Array');
  }
}

function assertBigint(value: unknown): asserts value is bigint {
  if (typeof value !== 'bigint') invalid('counters must be bigint values');
}

// Rust owns canonical encoding rules; these checks enforce the stricter JavaScript input types.
function assertVector(vector: VersionVector): void {
  if (!vector || typeof vector !== 'object' || !Array.isArray(vector.entries)) {
    invalid('entries must be an array');
  }
  for (const entry of vector.entries) {
    if (!entry || typeof entry !== 'object') invalid('entries must be objects');
    assertBytes(entry.replica);
    assertBigint(entry.frontier);
    if (!Array.isArray(entry.ranges)) invalid('ranges must be an array');
    for (const range of entry.ranges) {
      if (!Array.isArray(range) || range.length !== 2) invalid('ranges require exactly two bounds');
      assertBigint(range[0]);
      assertBigint(range[1]);
    }
  }
}

export function createVersionVectorCodecLoader(
  loadWasm: () => Promise<VersionVectorCodec>,
): () => Promise<VersionVectorCodec> {
  let codecPromise: Promise<VersionVectorCodec> | undefined;
  return () =>
    (codecPromise ??= loadWasm()
      .then((wasm) => ({
        encodeVersionVectorV0(vector: VersionVector): Uint8Array {
          assertVector(vector);
          try {
            return wasm.encodeVersionVectorV0(vector);
          } catch (error) {
            // The Rust bridge throws strings only for input-validation failures.
            if (typeof error === 'string') invalid(error);
            throw error;
          }
        },
        decodeVersionVectorV0(bytes: Uint8Array): VersionVector {
          assertBytes(bytes);
          try {
            return wasm.decodeVersionVectorV0(bytes);
          } catch (error) {
            if (typeof error === 'string') invalid(error);
            throw error;
          }
        },
      }))
      .catch((error: unknown) => {
        codecPromise = undefined;
        throw error;
      }));
}
