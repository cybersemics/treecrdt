import type { ReplicaId } from "./index.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function stripHexPrefix(hex: string): string {
  return hex.startsWith("0x") || hex.startsWith("0X") ? hex.slice(2) : hex;
}

export function bytesToHex(bytes: Uint8Array | ArrayLike<number>): string {
  const view = bytes instanceof Uint8Array ? bytes : Uint8Array.from(bytes);
  let out = "";
  for (const b of view) out += b.toString(16).padStart(2, "0");
  return out;
}

export function hexToBytes(hex: string): Uint8Array {
  const clean = stripHexPrefix(hex).trim();
  if (clean.length % 2 !== 0) throw new Error(`hex must have even length, got: ${hex}`);
  if (!/^[0-9a-fA-F]*$/.test(clean)) throw new Error(`invalid hex: ${hex}`);

  // Fast path for Node.js (Buffer is a Uint8Array subclass).
  const BufferCtor = (globalThis as any).Buffer as { from?: (data: string, encoding: string) => Uint8Array } | undefined;
  if (typeof BufferCtor?.from === "function") {
    return BufferCtor.from(clean, "hex");
  }

  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < clean.length; i += 2) {
    out[i / 2] = Number.parseInt(clean.slice(i, i + 2), 16);
  }
  return out;
}

export const ROOT_NODE_ID_HEX = "0".repeat(32);
export const TRASH_NODE_ID_HEX = "f".repeat(32);

/**
 * Normalize a NodeId into the canonical 16-byte (32 hex chars) big-endian form.
 *
 * Accepts:
 * - canonical 32-hex strings (with optional 0x prefix)
 * - decimal u128 strings
 * - unpadded hex (<= 32 hex chars, with optional 0x prefix)
 */
export function normalizeNodeId(nodeId: string): string {
  const raw = nodeId.trim();
  const clean = stripHexPrefix(raw);
  if (clean.length === 0) throw new Error("NodeId must not be empty");

  if (/^[0-9a-fA-F]{32}$/.test(clean)) return clean.toLowerCase();

  if (/^\d+$/.test(clean)) {
    const v = BigInt(clean);
    const maxU128 = (1n << 128n) - 1n;
    if (v < 0n || v > maxU128) throw new Error(`NodeId (decimal) out of u128 range: ${raw}`);
    return v.toString(16).padStart(32, "0");
  }

  if (!/^[0-9a-fA-F]+$/.test(clean)) {
    throw new Error(`NodeId must be hex (or decimal u128), got: ${raw}`);
  }

  let hex = clean.replace(/^0+/, "");
  if (hex.length === 0) hex = "0";
  if (hex.length > 32) throw new Error(`NodeId hex too long (expected <= 32 chars), got: ${raw}`);
  if (hex.length % 2 === 1) hex = `0${hex}`;
  return hex.padStart(32, "0").toLowerCase();
}

export function nodeIdToBytes16(nodeId: string): Uint8Array {
  const hex = normalizeNodeId(nodeId);
  const bytes = hexToBytes(hex);
  if (bytes.length !== 16) throw new Error(`NodeId must be 16 bytes, got ${bytes.length}`);
  return bytes;
}

export function nodeIdFromBytes16(bytes: Uint8Array): string {
  if (bytes.length !== 16) throw new Error(`NodeId must be 16 bytes, got ${bytes.length}`);
  return bytesToHex(bytes);
}

/**
 * Lenient decoding helper for values coming back from SQLite/JS bridges.
 * This mirrors the existing adapters' behavior: best-effort conversion to canonical NodeId hex.
 */
export function decodeNodeId(val: unknown): string {
  if (val === null || val === undefined) return "";
  if (typeof val === "bigint") return val.toString(16).padStart(32, "0");
  if (typeof val === "number") return BigInt(val).toString(16).padStart(32, "0");
  if (typeof val === "string") {
    const clean = val.trim();
    if (/^[0-9a-fA-F]{32}$/.test(clean)) return clean.toLowerCase();
    if (/^\d+$/.test(clean)) return BigInt(clean).toString(16).padStart(32, "0");
    return clean;
  }
  const bytes = val instanceof Uint8Array ? val : Uint8Array.from(val as any);
  return bytesToHex(bytes);
}

export function replicaIdToBytes(replica: ReplicaId): Uint8Array {
  return typeof replica === "string" ? textEncoder.encode(replica) : replica;
}

export function replicaIdFromBytes(bytes: Uint8Array): string {
  return textDecoder.decode(bytes);
}

export function decodeReplicaId(val: unknown): string {
  if (val === null || val === undefined) return "";
  if (typeof val === "string") return val;
  const bytes = val instanceof Uint8Array ? val : Uint8Array.from(val as any);
  return replicaIdFromBytes(bytes);
}
