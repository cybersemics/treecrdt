export function parseFlagValue(argv: string[], flag: string): string | undefined {
  const prefix = `${flag}=`;
  const raw = argv.find((arg) => arg.startsWith(prefix));
  return raw ? raw.slice(prefix.length).trim() : undefined;
}

export function parsePositiveIntFlag(
  argv: string[],
  flag: string,
  envName: string,
  fallback: number,
): number {
  const raw = parseFlagValue(argv, flag) ?? process.env[envName];
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`invalid ${flag} value "${raw}", expected a positive integer`);
  }
  return value;
}

export function parseNonNegativeIntFlag(
  argv: string[],
  flag: string,
  envName: string,
  fallback: number,
): number {
  const raw = parseFlagValue(argv, flag) ?? process.env[envName];
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`invalid ${flag} value "${raw}", expected a non-negative integer`);
  }
  return value;
}

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
  if (encoded.length === 0) throw new Error('label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

export function payloadBytesFromSeed(seed: number, size = 512): Uint8Array {
  if (!Number.isInteger(seed) || seed < 0) throw new Error(`invalid payload seed: ${seed}`);
  if (!Number.isInteger(size) || size <= 0) throw new Error(`invalid payload size: ${size}`);
  const out = new Uint8Array(size);
  for (let i = 0; i < out.length; i += 1) {
    out[i] = (seed + i * 31) % 251;
  }
  return out;
}
