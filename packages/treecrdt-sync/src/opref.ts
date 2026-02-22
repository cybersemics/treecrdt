import { blake3 } from '@noble/hashes/blake3';
import { utf8ToBytes } from '@noble/hashes/utils';

export const OPREF_V0_WIDTH_BYTES = 16;
const OPREF_V0_DOMAIN = utf8ToBytes('treecrdt/opref/v0');

export type OperationIdBytes = {
  replica: Uint8Array;
  counter: bigint | number;
};

function concatBytes(...parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((acc, p) => acc + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

function u32be(n: number): Uint8Array {
  if (!Number.isInteger(n) || n < 0 || n > 0xffff_ffff) throw new Error(`u32 out of range: ${n}`);
  const out = new Uint8Array(4);
  new DataView(out.buffer).setUint32(0, n, false);
  return out;
}

function normalizeU64(n: bigint | number): bigint {
  if (typeof n === 'number') {
    if (!Number.isSafeInteger(n) || n < 0)
      throw new Error(`u64 must be a safe non-negative integer, got: ${n}`);
    return BigInt(n);
  }
  if (n < 0n) throw new Error(`u64 must be non-negative, got: ${n}`);
  return n;
}

function u64be(n: bigint | number): Uint8Array {
  const v = normalizeU64(n);
  if (v > 0xffff_ffff_ffff_ffffn) throw new Error(`u64 out of range: ${v}`);
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, v, false);
  return out;
}

/**
 * Canonical v0 opId encoding:
 * `u32_be(len(replica)) || replica || u64_be(counter)`
 */
export function encodeOperationIdV0(id: OperationIdBytes): Uint8Array {
  return concatBytes(u32be(id.replica.length), id.replica, u64be(id.counter));
}

/**
 * Canonical v0 opRef derivation (16 bytes):
 * `blake3("treecrdt/opref/v0" || utf8(docId) || opIdBytes)[0..16]`
 */
export function deriveOpRefV0(docId: string, id: OperationIdBytes): Uint8Array {
  const docIdBytes = utf8ToBytes(docId);
  const opIdBytes = encodeOperationIdV0(id);
  return blake3(concatBytes(OPREF_V0_DOMAIN, docIdBytes, opIdBytes)).slice(0, OPREF_V0_WIDTH_BYTES);
}
