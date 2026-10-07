import { expect, test } from 'vitest';
import { bytesToHex } from '@treecrdt/interface/ids';
import { deriveOpRefV0 } from '@treecrdt/sync-protocol';
import { createMemoryClient } from '@treecrdt/wasm/memory';
import { createMemorySyncBackend } from '@treecrdt/wasm/sync';

test('indexes committed operations and preserves requested order', async () => {
  const memory = await createMemoryClient();
  try {
    const backend = createMemorySyncBackend(memory, { docId: 'document' });
    expect(await backend.listOpRefs({ all: {} })).toEqual([]);
    const inserted = memory.local.insert('0'.repeat(32), '1'.repeat(32));
    await backend.listOpRefs({ all: {} });
    const renamed = memory.local.payload('1'.repeat(32), Uint8Array.of(42));
    const refs = await backend.listOpRefs({ all: {} });
    expect(refs.map(bytesToHex)).toEqual(
      [inserted, renamed].map((op) => bytesToHex(deriveOpRefV0('document', op.meta.id))),
    );
    expect(await backend.getOpsByOpRefs([refs[1]!, refs[0]!, refs[1]!])).toEqual([
      renamed,
      inserted,
      renamed,
    ]);
    expect(await backend.maxLamport()).toBe(BigInt(renamed.meta.lamport));
    await expect(backend.getOpsByOpRefs([new Uint8Array(16)])).rejects.toThrow(/Missing/);
    await expect(backend.listOpRefs({ children: { parent: new Uint8Array(16) } })).rejects.toThrow(
      /full-document/,
    );
  } finally {
    memory.close();
  }
});
