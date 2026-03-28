export function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

export function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('replica label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}
