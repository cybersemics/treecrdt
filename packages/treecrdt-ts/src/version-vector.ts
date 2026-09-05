const FORMAT_MAGIC = Uint8Array.of(0x54, 0x43, 0x56, 0x56); // "TCVV"
const FORMAT_VERSION = 0;
const HEADER_BYTES = FORMAT_MAGIC.length + 1 + 4;
const ENTRY_FIXED_BYTES = 4 + 8 + 4;
const RANGE_BYTES = 8 + 8;
const MAX_U32 = 0xffff_ffff;
const MAX_U64 = (1n << 64n) - 1n;

export type VersionVectorRangeV0 = readonly [start: bigint, end: bigint];

export type VersionVectorEntryV0 = {
  readonly replica: Uint8Array;
  readonly frontier: bigint;
  readonly ranges: readonly VersionVectorRangeV0[];
};

export type VersionVectorV0 = {
  readonly entries: readonly VersionVectorEntryV0[];
};

export class VersionVectorV0CodecError extends Error {
  constructor(message: string) {
    super(`invalid VersionVectorV0: ${message}`);
    this.name = 'VersionVectorV0CodecError';
  }
}

function invalid(message: string): never {
  throw new VersionVectorV0CodecError(message);
}

function assertU32(value: number, field: string): void {
  if (!Number.isInteger(value) || value < 0 || value > MAX_U32) {
    invalid(`${field} exceeds the u32 range`);
  }
}

function assertU64(value: unknown, field: string): asserts value is bigint {
  if (typeof value !== 'bigint' || value < 0n || value > MAX_U64) {
    invalid(`${field} must be a bigint in the u64 range`);
  }
}

function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const length = Math.min(a.length, b.length);
  for (let index = 0; index < length; index += 1) {
    const difference = a[index]! - b[index]!;
    if (difference !== 0) return difference;
  }
  return a.length - b.length;
}

function isUint8Array(value: unknown): value is Uint8Array {
  return (
    ArrayBuffer.isView(value) && Object.prototype.toString.call(value) === '[object Uint8Array]'
  );
}

function validateEntry(entry: VersionVectorEntryV0, index: number): void {
  if (!entry || typeof entry !== 'object') invalid(`entries[${index}] must be an object`);
  if (!isUint8Array(entry.replica)) {
    invalid(`entries[${index}].replica must be a Uint8Array`);
  }
  assertU32(entry.replica.length, `entries[${index}].replica length`);
  assertU64(entry.frontier, `entries[${index}].frontier`);
  if (!Array.isArray(entry.ranges)) invalid(`entries[${index}].ranges must be an array`);
  assertU32(entry.ranges.length, `entries[${index}].ranges length`);

  if (entry.frontier === 0n && entry.ranges.length === 0) {
    invalid(`entries[${index}] is semantically empty`);
  }

  let previousEnd = entry.frontier;
  for (let rangeIndex = 0; rangeIndex < entry.ranges.length; rangeIndex += 1) {
    const range = entry.ranges[rangeIndex];
    if (!Array.isArray(range) || range.length !== 2) {
      invalid(`entries[${index}].ranges[${rangeIndex}] must contain exactly two bounds`);
    }
    const [start, end] = range;
    assertU64(start, `entries[${index}].ranges[${rangeIndex}][0]`);
    assertU64(end, `entries[${index}].ranges[${rangeIndex}][1]`);
    if (start === 0n) invalid(`entries[${index}].ranges[${rangeIndex}] starts at zero`);
    if (start > end) invalid(`entries[${index}].ranges[${rangeIndex}] has reversed bounds`);
    if (start <= previousEnd + 1n) {
      invalid(`entries[${index}].ranges[${rangeIndex}] is not normalized`);
    }
    previousEnd = end;
  }
}

function validateVersionVector(vector: VersionVectorV0): readonly VersionVectorEntryV0[] {
  if (!vector || typeof vector !== 'object' || !Array.isArray(vector.entries)) {
    invalid('entries must be an array');
  }
  assertU32(vector.entries.length, 'entries length');

  let previousReplica: Uint8Array | undefined;
  for (let index = 0; index < vector.entries.length; index += 1) {
    const entry = vector.entries[index]!;
    validateEntry(entry, index);
    if (previousReplica && compareBytes(previousReplica, entry.replica) >= 0) {
      invalid('replica ids must be unique and strictly sorted');
    }
    previousReplica = entry.replica;
  }
  return vector.entries;
}

function writeU32(view: DataView, offset: number, value: number): number {
  view.setUint32(offset, value, false);
  return offset + 4;
}

function writeU64(view: DataView, offset: number, value: bigint): number {
  view.setBigUint64(offset, value, false);
  return offset + 8;
}

/** Encode a canonical VersionVectorV0 value. */
export function encodeVersionVectorV0(vector: VersionVectorV0): Uint8Array {
  const entries = validateVersionVector(vector);
  let byteLength = BigInt(HEADER_BYTES);
  for (const entry of entries) {
    byteLength +=
      BigInt(ENTRY_FIXED_BYTES + entry.replica.length) +
      BigInt(RANGE_BYTES) * BigInt(entry.ranges.length);
  }
  if (byteLength > BigInt(Number.MAX_SAFE_INTEGER)) invalid('encoded value is too large');

  let bytes: Uint8Array;
  try {
    bytes = new Uint8Array(Number(byteLength));
  } catch {
    return invalid('encoded value is too large');
  }

  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let offset = 0;
  bytes.set(FORMAT_MAGIC, offset);
  offset += FORMAT_MAGIC.length;
  bytes[offset] = FORMAT_VERSION;
  offset += 1;
  offset = writeU32(view, offset, entries.length);

  for (const entry of entries) {
    offset = writeU32(view, offset, entry.replica.length);
    bytes.set(entry.replica, offset);
    offset += entry.replica.length;
    offset = writeU64(view, offset, entry.frontier);
    offset = writeU32(view, offset, entry.ranges.length);
    for (const [start, end] of entry.ranges) {
      offset = writeU64(view, offset, start);
      offset = writeU64(view, offset, end);
    }
  }
  return bytes;
}

class Reader {
  private offset = 0;
  private readonly view: DataView;

  constructor(private readonly bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  get remaining(): number {
    return this.bytes.length - this.offset;
  }

  readBytes(length: number, field: string): Uint8Array {
    if (length > this.remaining) invalid(`truncated ${field}`);
    const start = this.offset;
    this.offset += length;
    return this.bytes.slice(start, this.offset);
  }

  readU8(field: string): number {
    if (this.remaining < 1) invalid(`truncated ${field}`);
    return this.bytes[this.offset++]!;
  }

  readU32(field: string): number {
    if (this.remaining < 4) invalid(`truncated ${field}`);
    const value = this.view.getUint32(this.offset, false);
    this.offset += 4;
    return value;
  }

  readU64(field: string): bigint {
    if (this.remaining < 8) invalid(`truncated ${field}`);
    const value = this.view.getBigUint64(this.offset, false);
    this.offset += 8;
    return value;
  }
}

/** Decode and validate canonical VersionVectorV0 bytes. */
export function decodeVersionVectorV0(bytes: Uint8Array): VersionVectorV0 {
  if (!isUint8Array(bytes)) invalid('input must be a Uint8Array');
  bytes = new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const reader = new Reader(bytes);
  const magic = reader.readBytes(FORMAT_MAGIC.length, 'format marker');
  if (compareBytes(magic, FORMAT_MAGIC) !== 0) invalid('format marker mismatch');
  if (reader.readU8('format version') !== FORMAT_VERSION) invalid('unsupported format version');

  const entryCount = reader.readU32('entry count');
  if (entryCount > Math.floor(reader.remaining / ENTRY_FIXED_BYTES)) {
    invalid('truncated entries');
  }

  const entries: VersionVectorEntryV0[] = [];
  let previousReplica: Uint8Array | undefined;
  for (let entryIndex = 0; entryIndex < entryCount; entryIndex += 1) {
    const replicaLength = reader.readU32(`entries[${entryIndex}].replica length`);
    const replica = reader.readBytes(replicaLength, `entries[${entryIndex}].replica`);
    if (previousReplica && compareBytes(previousReplica, replica) >= 0) {
      invalid('replica ids must be unique and strictly sorted');
    }

    const frontier = reader.readU64(`entries[${entryIndex}].frontier`);
    const rangeCount = reader.readU32(`entries[${entryIndex}].range count`);
    if (rangeCount > Math.floor(reader.remaining / RANGE_BYTES)) {
      invalid(`truncated entries[${entryIndex}].ranges`);
    }

    const ranges: VersionVectorRangeV0[] = [];
    let previousEnd = frontier;
    for (let rangeIndex = 0; rangeIndex < rangeCount; rangeIndex += 1) {
      const start = reader.readU64(`entries[${entryIndex}].ranges[${rangeIndex}].start`);
      const end = reader.readU64(`entries[${entryIndex}].ranges[${rangeIndex}].end`);
      if (start === 0n) invalid(`entries[${entryIndex}].ranges[${rangeIndex}] starts at zero`);
      if (start > end) {
        invalid(`entries[${entryIndex}].ranges[${rangeIndex}] has reversed bounds`);
      }
      if (start <= previousEnd + 1n) {
        invalid(`entries[${entryIndex}].ranges[${rangeIndex}] is not normalized`);
      }
      ranges.push([start, end]);
      previousEnd = end;
    }

    if (frontier === 0n && ranges.length === 0) {
      invalid(`entries[${entryIndex}] is semantically empty`);
    }
    entries.push({ replica, frontier, ranges });
    previousReplica = replica;
  }

  if (reader.remaining !== 0) invalid('trailing bytes');
  return { entries };
}
