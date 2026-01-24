import type { ReplicaId } from "@treecrdt/interface";
import { replicaIdToBytes } from "@treecrdt/interface/ids";

const ORDER_KEY_DOMAIN = new TextEncoder().encode("treecrdt/order_key/v0");
const DIGIT_BYTES = 2;
const DEFAULT_BOUNDARY = 10;

const FNV_OFFSET_BASIS = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const MASK_64 = (1n << 64n) - 1n;

function u32ToBeBytes(n: number): Uint8Array {
  const out = new Uint8Array(4);
  const view = new DataView(out.buffer);
  view.setUint32(0, n >>> 0, false);
  return out;
}

function decodeDigits(bytes: Uint8Array): Uint16Array {
  if (!Number.isInteger(bytes.length / DIGIT_BYTES)) {
    throw new Error("order_key must have even length (u16 big-endian digits)");
  }
  const out = new Uint16Array(bytes.length / DIGIT_BYTES);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  for (let i = 0; i < out.length; i++) {
    out[i] = view.getUint16(i * DIGIT_BYTES, false);
  }
  return out;
}

function encodeDigits(digits: Uint16Array | number[]): Uint8Array {
  const out = new Uint8Array(digits.length * DIGIT_BYTES);
  const view = new DataView(out.buffer);
  for (let i = 0; i < digits.length; i++) {
    view.setUint16(i * DIGIT_BYTES, digits[i]!, false);
  }
  return out;
}

function sampleU64(seed: Uint8Array, depth: number): bigint {
  let h = FNV_OFFSET_BASIS;
  for (const b of ORDER_KEY_DOMAIN) {
    h ^= BigInt(b);
    h = (h * FNV_PRIME) & MASK_64;
  }
  for (const b of u32ToBeBytes(seed.length)) {
    h ^= BigInt(b);
    h = (h * FNV_PRIME) & MASK_64;
  }
  for (const b of seed) {
    h ^= BigInt(b);
    h = (h * FNV_PRIME) & MASK_64;
  }
  for (const b of u32ToBeBytes(depth)) {
    h ^= BigInt(b);
    h = (h * FNV_PRIME) & MASK_64;
  }
  return h;
}

function chooseSide(seed: Uint8Array, depth: number): boolean {
  // true = choose near left, false = choose near right
  return (sampleU64(seed, depth) & 1n) === 0n;
}

function chooseInRange(seed: Uint8Array, depth: number, lo: number, hi: number): number {
  if (lo > hi) throw new Error(`invalid range: ${lo}..${hi}`);
  if (lo === hi) return lo;
  const span = BigInt(hi - lo + 1);
  const n = Number(sampleU64(seed, depth) % span);
  return lo + n;
}

/**
 * Allocate a stable ordering key strictly between `left` and `right` (lexicographic order).
 *
 * Encoding matches core: variable-length sequence of big-endian u16 “digits”.
 * Allocation is LSEQ-inspired (bounded window near one side).
 */
export function allocateBetween(
  left: Uint8Array | null,
  right: Uint8Array | null,
  seed: Uint8Array
): Uint8Array {
  const leftDigits = decodeDigits(left ?? new Uint8Array());
  const rightDigits = decodeDigits(right ?? new Uint8Array());

  const out: number[] = [];
  let depth = 0;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const ld = depth < leftDigits.length ? leftDigits[depth]! : 0;
    const rd = depth < rightDigits.length ? rightDigits[depth]! : 0xffff;

    if (rd < ld) throw new Error("cannot allocate order_key: right < left");

    if (rd > ld + 1) {
      const gap = rd - ld - 1;
      const boundary = Math.min(DEFAULT_BOUNDARY, gap);
      const chooseLeft = chooseSide(seed, depth);

      let lo: number;
      let hi: number;
      if (gap > boundary) {
        if (chooseLeft) {
          lo = ld + 1;
          hi = ld + boundary;
        } else {
          lo = rd - boundary;
          hi = rd - 1;
        }
      } else {
        lo = ld + 1;
        hi = rd - 1;
      }

      out.push(chooseInRange(seed, depth, lo, hi));
      break;
    }

    out.push(ld);
    depth += 1;
  }

  return encodeDigits(out);
}

export function makeOrderKeySeed(replica: ReplicaId, counter: number): Uint8Array {
  const replicaBytes = replicaIdToBytes(replica);
  const out = new Uint8Array(replicaBytes.length + 8);
  out.set(replicaBytes, 0);
  const view = new DataView(out.buffer);
  view.setBigUint64(replicaBytes.length, BigInt(counter), false);
  return out;
}

function keyFor(orderKeys: Map<string, Uint8Array>, nodeId: string | null | undefined): Uint8Array | null {
  if (!nodeId) return null;
  return orderKeys.get(nodeId) ?? null;
}

/**
 * Given the current sibling list (node ids) for a parent, allocate an order_key
 * as if performing an `insert_after(parent, node, after)` / `move_after(node, parent, after)`.
 */
export function allocateOrderKeyAfter(opts: {
  siblings: string[];
  node: string;
  after: string | null;
  orderKeys: Map<string, Uint8Array>;
  seed: Uint8Array;
}): Uint8Array {
  const siblings = opts.siblings.filter((id) => id !== opts.node);

  let leftNode: string | null = null;
  let rightNode: string | null = null;

  if (opts.after) {
    const idx = siblings.indexOf(opts.after);
    if (idx === -1) throw new Error("after node is not a child of parent");
    leftNode = opts.after;
    rightNode = idx + 1 < siblings.length ? siblings[idx + 1]! : null;
  } else {
    rightNode = siblings.length > 0 ? siblings[0]! : null;
  }

  return allocateBetween(keyFor(opts.orderKeys, leftNode), keyFor(opts.orderKeys, rightNode), opts.seed);
}

