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

export type VersionVectorCodec = {
  encodeVersionVectorV0(vector: VersionVector): Uint8Array;
  decodeVersionVectorV0(bytes: Uint8Array): VersionVector;
};

function assertBytes(value: unknown): asserts value is Uint8Array {
  if (
    !ArrayBuffer.isView(value) ||
    Object.prototype.toString.call(value) !== '[object Uint8Array]'
  ) {
    throw new TypeError('bytes must be a Uint8Array');
  }
}

/** Create a lazy, cached loader for synchronous codec methods; failed loads can be retried. */
export function createVersionVectorCodecLoader(
  loadWasm: () => Promise<VersionVectorCodec>,
): () => Promise<VersionVectorCodec> {
  let codecPromise: Promise<VersionVectorCodec> | undefined;
  return () =>
    (codecPromise ??= loadWasm()
      .then((wasm) => ({
        encodeVersionVectorV0(vector: VersionVector): Uint8Array {
          return wasm.encodeVersionVectorV0(vector);
        },
        decodeVersionVectorV0(bytes: Uint8Array): VersionVector {
          assertBytes(bytes);
          return wasm.decodeVersionVectorV0(bytes);
        },
      }))
      .catch((error: unknown) => {
        codecPromise = undefined;
        throw error;
      }));
}
