import { readFile } from 'node:fs/promises';
import { expect, test, vi } from 'vitest';
import { createMemoryClient } from '../src/memory-browser.js';
import { loadVersionVectorCodec } from '../src/codec-browser.js';

test('shares lazy browser WASM initialization with the codec and retries failed loads', async () => {
  const unavailable = new Error('WASM unavailable');
  const fetchWasm = vi.fn().mockRejectedValueOnce(unavailable);
  vi.stubGlobal('fetch', fetchWasm);
  try {
    expect(fetchWasm).not.toHaveBeenCalled();
    await Promise.all([
      expect(createMemoryClient()).rejects.toBe(unavailable),
      expect(loadVersionVectorCodec()).rejects.toBe(unavailable),
    ]);
    expect(fetchWasm).toHaveBeenCalledTimes(1);

    const bytes = await readFile(new URL('../pkg-web/treecrdt_wasm_bg.wasm', import.meta.url));
    fetchWasm.mockImplementation(
      async () => new Response(bytes, { headers: { 'Content-Type': 'application/wasm' } }),
    );
    const replica = new Uint8Array(32).fill(7);
    const first = createMemoryClient({ replicaId: replica });
    replica.fill(99);
    const [codec, source, receiver] = await Promise.all([
      loadVersionVectorCodec(),
      first,
      createMemoryClient(),
    ]);
    try {
      const operation = source.local.insert('0'.repeat(32), '1'.repeat(32));
      expect(operation.meta.id.replica).toEqual(new Uint8Array(32).fill(7));
      expect(source.get('1'.repeat(32))?.parentId).toBe('0'.repeat(32));
      expect(receiver.get('1'.repeat(32))).toBeUndefined();
      receiver.appendOperations([operation]);
      expect(receiver.get('1'.repeat(32))?.parentId).toBe('0'.repeat(32));
      expect(codec.decodeVersionVector(codec.encodeVersionVector({ entries: [] }))).toEqual({
        entries: [],
      });
      expect(fetchWasm).toHaveBeenCalledTimes(2);
    } finally {
      source.close();
      receiver.close();
    }
  } finally {
    vi.unstubAllGlobals();
  }
});
