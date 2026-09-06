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

type WasmVersionVector = {
  entries: { replica: number[]; frontier: bigint; ranges: VersionVectorRange[] }[];
};

type WasmCodec = {
  encodeVersionVectorV0(vector: WasmVersionVector): Uint8Array;
  decodeVersionVectorV0(bytes: Uint8Array): WasmVersionVector;
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

// Check JS types and snapshot caller-owned values before asynchronous loading. Rust owns all
// canonical encoding rules, including ordering, normalized ranges, and counter bounds.
function snapshotVector(vector: VersionVector): WasmVersionVector {
  if (!vector || typeof vector !== 'object' || !Array.isArray(vector.entries)) {
    invalid('entries must be an array');
  }
  return {
    entries: vector.entries.map((entry: VersionVectorEntry) => {
      if (!entry || typeof entry !== 'object') invalid('entries must be objects');
      assertBytes(entry.replica);
      assertBigint(entry.frontier);
      if (!Array.isArray(entry.ranges)) invalid('ranges must be an array');
      return {
        replica: Array.from(entry.replica),
        frontier: entry.frontier,
        ranges: entry.ranges.map((range: VersionVectorRange) => {
          if (!Array.isArray(range) || range.length !== 2)
            invalid('ranges require exactly two bounds');
          assertBigint(range[0]);
          assertBigint(range[1]);
          return [range[0], range[1]];
        }),
      };
    }),
  };
}

export function createVersionVectorCodec(loadWasm: () => Promise<WasmCodec>) {
  let wasmPromise: Promise<WasmCodec> | undefined;
  const getWasm = () =>
    (wasmPromise ??= loadWasm().catch((error: unknown) => {
      wasmPromise = undefined;
      throw error;
    }));

  return {
    /** Encode canonical VersionVectorV0 bytes using the Rust core codec. */
    async encodeVersionVectorV0(vector: VersionVector): Promise<Uint8Array> {
      const input = snapshotVector(vector);
      const wasm = await getWasm();
      try {
        return wasm.encodeVersionVectorV0(input);
      } catch (error) {
        // The Rust bridge throws strings only for input-validation failures.
        if (typeof error === 'string') invalid(error);
        throw error;
      }
    },

    /** Decode and validate canonical VersionVectorV0 bytes using the Rust core codec. */
    async decodeVersionVectorV0(bytes: Uint8Array): Promise<VersionVector> {
      assertBytes(bytes);
      const input = new Uint8Array(bytes);
      const wasm = await getWasm();
      let vector: WasmVersionVector;
      try {
        vector = wasm.decodeVersionVectorV0(input);
      } catch (error) {
        if (typeof error === 'string') invalid(error);
        throw error;
      }
      return {
        entries: vector.entries.map((entry) => ({
          ...entry,
          replica: Uint8Array.from(entry.replica),
        })),
      };
    },
  };
}
