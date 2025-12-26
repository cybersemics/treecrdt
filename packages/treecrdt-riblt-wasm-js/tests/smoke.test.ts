import { expect, test } from "vitest";

import { RibltDecoder16, RibltEncoder16 } from "../dist/index.js";

function u64be16(n: bigint): Uint8Array {
  const out = new Uint8Array(16);
  let x = n;
  for (let i = 7; i >= 0; i -= 1) {
    out[i] = Number(x & 0xffn);
    x >>= 8n;
  }
  return out;
}

function readU64be(bytes16: Uint8Array): bigint {
  let n = 0n;
  for (let i = 0; i < 8; i += 1) {
    n = (n << 8n) | BigInt(bytes16[i]);
  }
  return n;
}

test("riblt wasm: reconciles symmetric difference", () => {
  const alice = [1n, 2n, 3n].map(u64be16);
  const bob = [1n, 3n, 4n].map(u64be16);

  const enc = new RibltEncoder16();
  for (const s of alice) enc.addSymbol(s);

  const dec = new RibltDecoder16();
  for (const s of bob) dec.addLocalSymbol(s);

  let steps = 0;
  do {
    steps += 1;
    dec.addCodeword(enc.nextCodeword());
    dec.tryDecode();
    if (steps > 50) throw new Error("decode did not converge");
  } while (!dec.decoded());

  const remoteOnly = dec.remoteMissing() as Uint8Array[];
  const localOnly = dec.localMissing() as Uint8Array[];

  expect(remoteOnly.map(readU64be)).toEqual([2n]);
  expect(localOnly.map(readU64be)).toEqual([4n]);
});
