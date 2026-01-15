function padBase64(s: string): string {
  const rem = s.length % 4;
  if (rem === 0) return s;
  return s + "=".repeat(4 - rem);
}

export function base64urlEncode(bytes: Uint8Array): string {
  const BufferCtor = (globalThis as any).Buffer as { from?: (data: Uint8Array) => { toString: (enc: string) => string } } | undefined;
  if (typeof BufferCtor?.from === "function") {
    return BufferCtor.from(bytes).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
  }

  let bin = "";
  for (let i = 0; i < bytes.length; i += 1) bin += String.fromCharCode(bytes[i]!);
  const b64 = globalThis.btoa(bin);
  return b64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

export function base64urlDecode(input: string): Uint8Array {
  const s = padBase64(input.replace(/-/g, "+").replace(/_/g, "/"));

  const BufferCtor = (globalThis as any).Buffer as { from?: (data: string, enc: string) => Uint8Array } | undefined;
  if (typeof BufferCtor?.from === "function") {
    return BufferCtor.from(s, "base64");
  }

  const bin = globalThis.atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i += 1) out[i] = bin.charCodeAt(i);
  return out;
}

