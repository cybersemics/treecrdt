import { expect, test } from 'vitest';

import { bytesToHex, nodeIdToBytes16 } from '@treecrdt/interface/ids';
import type { SqliteRunner } from '@treecrdt/interface/sqlite';

import { createTreecrdtSyncBackendFromClient } from '../dist/backend.js';
import { deriveOpRefV0 } from '../dist/opref.js';
import type { OpRef } from '../dist/types.js';

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('replica label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

test("backend children(parent) includes the parent's latest payload opRef", async () => {
  const docId = 'doc-backend-payload-root';
  const parentHex = '11'.repeat(16);
  const parentBytes = nodeIdToBytes16(parentHex);

  const replica = replicaFromLabel('r');
  const counter = 42n;
  const payloadWriterOpRef = deriveOpRefV0(docId, { replica, counter });
  const payloadWriterHex = bytesToHex(payloadWriterOpRef);

  const childRefs: OpRef[] = [new Uint8Array(16).fill(7)];

  const runnerCalls: string[] = [];
  const runner: SqliteRunner = {
    exec: async () => {},
    getText: async (sql: string, params?: unknown[]) => {
      runnerCalls.push(sql);
      if (sql.includes('treecrdt_ensure_materialized')) return '1';
      if (sql.includes('FROM tree_payload')) {
        expect(params?.[0]).toEqual(parentBytes);
        return JSON.stringify({ replica: bytesToHex(replica), counter: counter.toString() });
      }
      return null;
    },
  };

  const backend = createTreecrdtSyncBackendFromClient(
    {
      runner,
      opRefs: {
        all: async () => [],
        children: async (parentHexArg) => {
          expect(parentHexArg).toBe(parentHex);
          return childRefs;
        },
      },
      ops: {
        get: async () => [],
        appendMany: async () => {},
      },
    },
    docId,
  );

  const refs = await backend.listOpRefs({ children: { parent: parentBytes } });
  expect(refs.map(bytesToHex)).toContain(payloadWriterHex);
  expect(runnerCalls.some((s) => s.includes('treecrdt_ensure_materialized'))).toBe(true);
  expect(runnerCalls.some((s) => s.includes('FROM tree_payload'))).toBe(true);
});

test("backend children(parent) does not duplicate the parent's payload opRef", async () => {
  const docId = 'doc-backend-payload-dedup';
  const parentHex = '22'.repeat(16);
  const parentBytes = nodeIdToBytes16(parentHex);

  const replica = replicaFromLabel('x');
  const counter = 1n;
  const payloadWriterOpRef = deriveOpRefV0(docId, { replica, counter });
  const payloadWriterHex = bytesToHex(payloadWriterOpRef);

  const childRefs: OpRef[] = [new Uint8Array(16).fill(8), payloadWriterOpRef];

  const runner: SqliteRunner = {
    exec: async () => {},
    getText: async (sql: string) => {
      if (sql.includes('treecrdt_ensure_materialized')) return '1';
      if (sql.includes('FROM tree_payload')) {
        return JSON.stringify({ replica: bytesToHex(replica), counter: counter.toString() });
      }
      return null;
    },
  };

  const backend = createTreecrdtSyncBackendFromClient(
    {
      runner,
      opRefs: {
        all: async () => [],
        children: async () => childRefs,
      },
      ops: {
        get: async () => [],
        appendMany: async () => {},
      },
    },
    docId,
  );

  const refs = await backend.listOpRefs({ children: { parent: parentBytes } });
  expect(refs.map(bytesToHex).filter((h) => h === payloadWriterHex).length).toBe(1);
});

test('backend children(parent) falls back to ops table when tree_payload is missing', async () => {
  const docId = 'doc-backend-payload-fallback';
  const parentHex = '33'.repeat(16);
  const parentBytes = nodeIdToBytes16(parentHex);

  const payloadWriterOpRef: OpRef = new Uint8Array(16).fill(9);
  const payloadWriterHex = bytesToHex(payloadWriterOpRef);

  const childRefs: OpRef[] = [new Uint8Array(16).fill(1)];

  const runnerCalls: string[] = [];
  const runner: SqliteRunner = {
    exec: async () => {},
    getText: async (sql: string) => {
      runnerCalls.push(sql);
      if (sql.includes('treecrdt_ensure_materialized')) return '1';
      if (sql.includes('FROM tree_payload')) return null;
      if (sql.includes('FROM ops')) return payloadWriterHex;
      return null;
    },
  };

  const backend = createTreecrdtSyncBackendFromClient(
    {
      runner,
      opRefs: {
        all: async () => [],
        children: async () => childRefs,
      },
      ops: {
        get: async () => [],
        appendMany: async () => {},
      },
    },
    docId,
  );

  const refs = await backend.listOpRefs({ children: { parent: parentBytes } });
  expect(refs.map(bytesToHex)).toContain(payloadWriterHex);
  expect(runnerCalls.some((s) => s.includes('FROM tree_payload'))).toBe(true);
  expect(runnerCalls.some((s) => s.includes('FROM ops'))).toBe(true);
});
