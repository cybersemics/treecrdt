import { expect, test } from 'vitest';
import { bytesToHex } from '@treecrdt/interface/ids';
import { deriveOpRefV0 } from '@treecrdt/sync-protocol';
import { createMemoryClient } from '@treecrdt/wasm/memory';
import { createTreecrdtSyncBackendFromClient } from '@treecrdt/sync-protocol/backend';

test('indexes committed operations and preserves requested order for full-document sync', async () => {
  const memory = await createMemoryClient({ docId: 'document' });
  try {
    expect(() => createTreecrdtSyncBackendFromClient(memory, 'other-document')).toThrow(
      /document id/,
    );
    const backend = createTreecrdtSyncBackendFromClient(memory, memory.docId);
    expect(await backend.listOpRefs({ all: {} })).toEqual([]);
    const inserted = memory.local.insert('0'.repeat(32), '1'.repeat(32), { type: 'last' }, null);
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
    // An observed duplicate can advance the clock without changing the stored head or refs.
    await backend.applyOps([{ ...renamed, meta: { ...renamed.meta, lamport: 100 } }]);
    expect(await backend.maxLamport()).toBe(BigInt(renamed.meta.lamport));
    expect(await backend.listOpRefs({ all: {} })).toEqual(refs);
    expect(await backend.getOpsByOpRefs([refs[1]!])).toEqual([renamed]);
    await expect(backend.getOpsByOpRefs([new Uint8Array(16)])).rejects.toThrow(/Missing/);
    await expect(backend.listOpRefs({ children: { parent: new Uint8Array(16) } })).rejects.toThrow(
      /full-document/,
    );
    memory.close();
    await expect(backend.listOpRefs({ all: {} })).rejects.toThrow('closed');
    await expect(backend.getOpsByOpRefs(refs)).rejects.toThrow('closed');
  } finally {
    memory.close();
  }
});
