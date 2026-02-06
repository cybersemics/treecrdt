export function concatBytes(...parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((acc, p) => acc + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

export function u8(n: number): Uint8Array {
  if (!Number.isInteger(n) || n < 0 || n > 0xff) throw new Error(`u8 out of range: ${n}`);
  return new Uint8Array([n]);
}

export function u32be(n: number): Uint8Array {
  if (!Number.isInteger(n) || n < 0 || n > 0xffff_ffff) throw new Error(`u32 out of range: ${n}`);
  const out = new Uint8Array(4);
  new DataView(out.buffer).setUint32(0, n, false);
  return out;
}

export function u64be(n: bigint | number): Uint8Array {
  const v = typeof n === "bigint" ? n : BigInt(n);
  if (v < 0n || v > 0xffff_ffff_ffff_ffffn) throw new Error(`u64 out of range: ${v}`);
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, v, false);
  return out;
}
