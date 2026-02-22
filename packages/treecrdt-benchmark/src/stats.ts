function readEnv(name: string): string | undefined {
  const env = (globalThis as any)?.process?.env as Record<string, string | undefined> | undefined;
  const raw = env?.[name];
  return typeof raw === 'string' && raw.length > 0 ? raw : undefined;
}

export function envInt(name: string): number | undefined {
  const raw = readEnv(name);
  if (!raw) return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

export function envIntList(name: string): number[] | undefined {
  const raw = readEnv(name);
  if (!raw) return undefined;
  const parts = raw
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  if (parts.length === 0) return undefined;

  const values: number[] = [];
  for (const part of parts) {
    const n = Number(part);
    if (!Number.isFinite(n) || !Number.isInteger(n)) {
      throw new Error(`${name}: invalid integer value: ${part}`);
    }
    values.push(n);
  }
  return values;
}

export function quantile(values: number[], q: number): number {
  if (values.length === 0) return NaN;
  if (!(q >= 0 && q <= 1)) throw new Error(`q must be in [0,1], got: ${q}`);
  const sorted = [...values].sort((a, b) => a - b);
  const idx = (sorted.length - 1) * q;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo]!;
  const w = idx - lo;
  return sorted[lo]! * (1 - w) + sorted[hi]! * w;
}
