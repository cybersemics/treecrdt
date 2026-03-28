import { randomUUID } from 'node:crypto';

import { beforeAll, beforeEach, describe, expect, test } from 'vitest';

import { nodeIdToBytes16 } from '@treecrdt/interface/ids';
import { makeOp, nodeIdFromInt } from '@treecrdt/benchmark';

import { createPostgresNapiSyncBackendFactory } from '../dist/index.js';

const POSTGRES_URL = process.env.TREECRDT_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

maybeDescribe('postgres-napi sync backend bridge', () => {
  const docA = `doc-${randomUUID()}`;
  const docB = `doc-${randomUUID()}`;
  const root = '0'.repeat(32);
  const replicaA = replicaFromLabel('a');
  const replicaB = replicaFromLabel('b');
  let factory!: ReturnType<typeof createPostgresNapiSyncBackendFactory>;

  beforeAll(async () => {
    factory = createPostgresNapiSyncBackendFactory(POSTGRES_URL!);
    await factory.ensureSchema();
  });

  beforeEach(async () => {
    await factory.resetDocForTests(docA);
    await factory.resetDocForTests(docB);
  });

  test('roundtrips through sync backend surface', async () => {
    const backend = await factory.open(docA);
    const node = nodeIdFromInt(1);

    await backend.applyOps([
      makeOp(replicaA, 1, 1, {
        type: 'insert',
        parent: root,
        node,
        orderKey: orderKeyFromPosition(0),
      }),
      makeOp(replicaA, 2, 2, {
        type: 'payload',
        node,
        payload: new Uint8Array([7]),
      }),
    ]);

    const refsAll = await backend.listOpRefs({ all: {} });
    expect(refsAll).toHaveLength(2);

    const ops = await backend.getOpsByOpRefs(refsAll);
    expect(ops).toHaveLength(2);
    expect(new Set(ops.map((op) => op.kind.type))).toEqual(new Set(['insert', 'payload']));

    const refsChildren = await backend.listOpRefs({ children: { parent: nodeIdToBytes16(root) } });
    expect(Array.isArray(refsChildren)).toBe(true);

    expect(await backend.maxLamport()).toBe(2n);
  });

  test('doc isolation is preserved through factory/open', async () => {
    const a = await factory.open(docA);
    const b = await factory.open(docB);

    await a.applyOps([
      makeOp(replicaA, 1, 1, {
        type: 'insert',
        parent: root,
        node: nodeIdFromInt(11),
        orderKey: orderKeyFromPosition(0),
      }),
    ]);

    await b.applyOps([
      makeOp(replicaB, 1, 1, {
        type: 'insert',
        parent: root,
        node: nodeIdFromInt(22),
        orderKey: orderKeyFromPosition(0),
      }),
    ]);

    expect(await a.listOpRefs({ all: {} })).toHaveLength(1);
    expect(await b.listOpRefs({ all: {} })).toHaveLength(1);
  });
});
