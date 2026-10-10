import { afterEach, expect, test, vi } from 'vitest';
import { WasmTree } from '../pkg/treecrdt_wasm.js';
import {
  createInitializedMemoryClient,
  type MemoryClient,
  type MemoryContent,
} from '../src/memory-client.js';

const root = '0'.repeat(32);
const a = '1'.padStart(32, '0');
const b = '2'.padStart(32, '0');
const c = '3'.padStart(32, '0');
const clients: MemoryClient[] = [];
const options = {
  decode: ({ id, parentId, payload }: MemoryContent) =>
    payload ? Object.freeze({ id, parentId, group: payload[0], value: payload[1] }) : undefined,
};

function open(replica = '01') {
  const native = new WasmTree(replica.repeat(32));
  const client = createInitializedMemoryClient(native);
  clients.push(client);
  return { client, native };
}

afterEach(() => {
  for (const client of clients.splice(0)) client.close();
  vi.restoreAllMocks();
});

test('validates existing rows once and reuses unaffected decoded values and child lists', () => {
  const { client, native } = open();
  client.transact(({ local }) => {
    local.insert(root, a, null, Uint8Array.of(0, 0));
    local.insert(a, b, null, Uint8Array.of(1, 1));
    local.insert(a, c, b, Uint8Array.of(2, 2));
  });
  const enumerate = vi.spyOn(native, 'nodeIds');
  const decode = vi.fn(options.decode);
  const projection = client.createProjection({ ...options, decode });
  expect(enumerate).toHaveBeenCalledTimes(1);
  const held = projection.get(c);
  const children = projection.getChildren(a);
  enumerate.mockClear();
  decode.mockClear();
  expect(projection.get(c)).toBe(held);
  expect(projection.getChildren('1')).toBe(children);
  expect(decode).not.toHaveBeenCalled();

  client.local.payload(b, Uint8Array.of(1, 4));
  expect(projection.get(b)?.value).toBe(4);
  expect(projection.get(c)).toBe(held);
  expect(projection.getChildren(a)).toBe(children);
  expect(enumerate).not.toHaveBeenCalled();
  client.local.delete(b);
  expect(projection.get(b)).toBeUndefined();
  expect(projection.getChildren(a)).toEqual([c]);
  client.close();
  expect(() => projection.get(c)).toThrow('closed');
});

test('content and A-to-B-to-A edits are visible within one transaction', () => {
  const { client } = open();
  client.local.insert(root, a, null, Uint8Array.of(1, 1));
  client.local.insert(root, b, a, Uint8Array.of(0, 0));
  const projection = client.createProjection(options);
  const revision = projection.revision;
  client.transact(({ local }) => {
    local.payload(a, Uint8Array.of(1, 5));
    expect(projection.get(a)?.value).toBe(5);
    expect(projection.revision).toBeGreaterThan(revision);
    const metadataRevision = projection.revision;
    local.move(a, b);
    expect(projection.get(a)?.parentId).toBe(b);
    expect(projection.revision).toBeGreaterThan(metadataRevision);
    local.payload(a, Uint8Array.of(2, 7));
    expect(projection.get(a)?.group).toBe(2);
    local.payload(a, Uint8Array.of(1, 1));
    expect(projection.get(a)?.value).toBe(1);
  });
  expect(projection.get(a)?.value).toBe(1);
});

test('filters decoded visibility and reuses native child ordering without redundant invalidation', () => {
  const { client } = open();
  client.transact(({ local }) => {
    local.insert(root, a, null, Uint8Array.of(0, 0));
    local.insert(a, b);
    local.insert(a, c, b, Uint8Array.of(1, 2));
  });
  const projection = client.createProjection(options);
  const visible = projection.getChildren(a);
  expect(visible).toEqual([c]);
  client.local.move(b, a, c);
  expect(projection.getChildren(a)).toBe(visible);
  client.local.move(b, a, null);
  expect(projection.getChildren(a)).toBe(visible);
  client.transact(({ local }) => {
    local.payload(b, Uint8Array.of(1, 3));
    expect(projection.getChildren(a)).toEqual([b, c]);
    local.move(c, a, null);
    expect(projection.getChildren(a)).toEqual([c, b]);
    local.move(c, a, b);
    expect(projection.getChildren(a)).toEqual([b, c]);
    local.payload(c, null);
    expect(projection.getChildren(a)).toEqual([b]);
    local.payload(a, null);
    expect(projection.getChildren(a)).toEqual([]);
    local.payload(a, Uint8Array.of(0, 0));
    expect(projection.getChildren(a)).toEqual([b]);
  });
});

test('rollback discards reset and incremental projections after callback and caught native failures', () => {
  const { client, native } = open();
  client.local.insert(root, a, null, Uint8Array.of(1, 1));
  const projection = client.createProjection(options);
  const held = projection.get(a);
  let provisionalRevision = projection.revision;
  expect(() =>
    client.transact(({ local }) => {
      // Re-enabling the native tracker exercises a full-refresh batch over an already queried projection.
      native.enableReadTracking();
      local.payload(a, Uint8Array.of(1, 5));
      expect(projection.get(a)?.value).toBe(5);
      local.payload(a, Uint8Array.of(2, 9));
      expect(projection.get(a)?.group).toBe(2);
      provisionalRevision = projection.revision;
      throw new Error('cancel command');
    }),
  ).toThrow('cancel command');
  expect(projection.get(a)).toEqual(held);
  expect(projection.revision).toBeGreaterThan(provisionalRevision);
  expect(() =>
    client.transact(({ local }) => {
      local.payload(a, Uint8Array.of(2, 9));
      expect(projection.get(a)?.group).toBe(2);
      expect(() => local.move(a, root, b)).toThrow();
    }),
  ).toThrow();
  expect(projection.get(a)?.group).toBe(1);
});

test.each([false, true])('decoder failures cannot commit invalid content (reset: %s)', (reset) => {
  const { client, native } = open();
  client.local.insert(root, a, null, Uint8Array.of(1, 1));
  const projection = client.createProjection({
    ...options,
    decode: (content) => {
      if (content.payload?.[1] === 9) throw new Error('invalid content');
      return options.decode(content);
    },
  });
  expect(() =>
    client.transact(({ local }) => {
      if (reset) native.enableReadTracking();
      expect(() => {
        local.payload(a, Uint8Array.of(2, 9));
        return projection.get(a);
      }).toThrow('invalid content');
    }),
  ).toThrow();
  expect(client.getContent(a)?.payload).toEqual(Uint8Array.of(1, 1));
  expect(projection.get(a)?.value).toBe(1);
  // Commit validates changed rows even when the command does not read the projection.
  expect(() => client.local.insert(root, b, a, Uint8Array.of(1, 9))).toThrow('invalid content');
  expect(projection.get(b)).toBeUndefined();
  client.local.payload(a, Uint8Array.of(1, 2));
  expect(projection.get(a)?.value).toBe(2);
});

test('projection callbacks must be synchronous and cannot author writes even when the error is caught', () => {
  const { client } = open();
  client.local.insert(root, a, null, Uint8Array.of(1, 1));
  expect(() => client.transact(() => client.createProjection(options))).toThrow();
  expect(() =>
    client.createProjection({
      ...options,
      // @ts-expect-error Promise-returning decoders violate the synchronous callback contract.
      decode: async (content) => options.decode(content),
    }),
  ).toThrow('synchronous');
  const projection = client.createProjection({
    decode: (content) => {
      if (content.payload?.[1] === 9) {
        expect(() => client.local.payload(a, Uint8Array.of(1, 3))).toThrow();
      }
      return options.decode(content);
    },
  });
  expect(() => client.local.payload(a, Uint8Array.of(1, 9))).toThrow();
  expect(projection.get(a)?.value).toBe(1);
});

test('incoming operations refresh projections before client subscribers run', () => {
  const { client } = open();
  client.local.insert(root, a, null, Uint8Array.of(1, 1));
  const { client: remote } = open('02');
  remote.appendOperations(client.operationsFrom(0));
  const projection = client.createProjection(options);
  const observed: unknown[] = [];
  client.subscribe(() => observed.push(projection.get(a)?.value));
  client.appendOperations([remote.local.payload(a, Uint8Array.of(2, 8))]);
  expect(observed).toEqual([8]);
});
