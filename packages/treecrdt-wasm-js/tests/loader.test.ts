import { readFile } from 'node:fs/promises';
import { expect, test, vi } from 'vitest';
import { createMemoryClient, initializeMemoryWasm } from '../dist/index.browser.js';

test('explicit browser initialization accepts bytes, shares initialization, and can retry after failure', async () => {
  const fetch = vi.spyOn(globalThis, 'fetch');
  await expect(initializeMemoryWasm(new Uint8Array([0]))).rejects.toThrow();
  const bytes = await readFile(new URL('../pkg-web/treecrdt_wasm_bg.wasm', import.meta.url));
  const first = initializeMemoryWasm(bytes);
  expect(initializeMemoryWasm(bytes)).toBe(first);
  await first;
  const client = await createMemoryClient();
  try {
    const operation = client.local.insert('0'.repeat(32), '1'.repeat(32));
    expect(operation.kind.type).toBe('insert');
    expect(client.operationCount()).toBe(1);
    expect(fetch).not.toHaveBeenCalled();
  } finally {
    client.close();
  }
});
