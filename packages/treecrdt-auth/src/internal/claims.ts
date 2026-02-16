export function toNumber(val: unknown, field: string): number | undefined {
  if (val === undefined || val === null) return undefined;
  if (typeof val === "number") return val;
  if (typeof val === "bigint") {
    if (val > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error(`${field} too large`);
    return Number(val);
  }
  throw new Error(`${field} must be a number`);
}

export function mapGet(map: Map<unknown, unknown>, key: unknown): unknown {
  return map.has(key) ? map.get(key) : undefined;
}

export function getClaim(map: Map<unknown, unknown>, numKey: number, strKey: string): unknown {
  return mapGet(map, numKey) ?? mapGet(map, strKey);
}

export function getField(obj: unknown, key: string): unknown {
  if (!obj || typeof obj !== "object") return undefined;
  if (obj instanceof Map) return mapGet(obj, key);
  return (obj as any)[key];
}
