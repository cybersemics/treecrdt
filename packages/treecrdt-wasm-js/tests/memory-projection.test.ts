import { afterEach, expect, test, vi } from 'vitest';
import { WasmTree } from '../pkg/treecrdt_wasm.js';
import {
  createInitializedMemoryClient,
  type MemoryClient,
  type MemoryContent,
  type MemoryChanges,
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

test('materialization scopes coalesce writes until reads and receive incremental owned batches', () => {
  const { client, native } = open();
  client.local.insert(root, a, null, Uint8Array.of(1));
  let value: number | undefined;
  const batches: Pick<MemoryChanges, 'reset' | 'changes'>[] = [];
  const rollback = vi.fn();
  const start = vi.fn(() => {
    const previous = value;
    return {
      apply: (batch: Pick<MemoryChanges, 'reset' | 'changes'>) => {
        batches.push(batch);
        if (batch.reset) value = client.getContent(a)?.payload?.[0];
        else for (const row of batch.changes) if (row.id === a) value = row.after?.payload?.[0];
      },
      rollback: () => {
        rollback();
        value = previous;
      },
    };
  });
  const observer = client.observeMaterialization(start);
  expect(value).toBe(1);
  expect(batches).toEqual([{ reset: true, changes: [] }]);
  batches.length = 0;
  const drain = vi.spyOn(native, 'drainReadChanges');
  const published = vi.fn(() => value);
  client.subscribe(published);
  const result = client.transact(({ local }) => {
    local.payload(a, Uint8Array.of(2));
    local.payload(a, Uint8Array.of(3));
    expect(value).toBe(1);
    expect(drain).not.toHaveBeenCalled();
    observer.flush();
    expect(value).toBe(3);
    observer.flush();
    expect(drain).toHaveBeenCalledTimes(1);
    local.payload(a, Uint8Array.of(1));
    observer.flush();
    expect(value).toBe(1);
  });
  expect(start).toHaveBeenCalledTimes(2);
  expect(rollback).not.toHaveBeenCalled();
  expect(batches.map((batch) => batch.changes.find((change) => change.id === a))).toMatchObject([
    { before: { payload: Uint8Array.of(1) }, after: { payload: Uint8Array.of(3) } },
    { before: { payload: Uint8Array.of(3) }, after: { payload: Uint8Array.of(1) } },
  ]);
  expect(() => (batches[0].changes as unknown[]).push(null)).toThrow();
  const held = batches[0].changes.find((change) => change.id === a)!;
  held.after!.payload!.fill(99);
  expect(held.after!.payload).toEqual(Uint8Array.of(3));
  expect(result.changes.changes).toEqual([]);
  expect(published).not.toHaveBeenCalled();
  const { client: remote } = open('02');
  remote.appendOperations(client.operationsFrom(0));
  client.appendOperations([remote.local.payload(a, Uint8Array.of(4))]);
  expect(published).toHaveReturnedWith(4);
  client.close();
  expect(() => observer.flush()).toThrow('closed');
});

test('caught partial apply failures restore every scope and poison further writes', () => {
  const { client } = open();
  client.local.insert(root, a, null, Uint8Array.of(1));
  const values = [0, 0];
  const rollbacks = [vi.fn(), vi.fn()];
  const observers = values.map((_, index) =>
    client.observeMaterialization(() => {
      const previous = values[index];
      return {
        apply: () => {
          values[index] = client.getContent(a)!.payload![0];
          if (index === 1 && values[index] === 9) throw new Error('invalid derived value');
        },
        rollback: () => {
          rollbacks[index]();
          values[index] = previous;
        },
      };
    }),
  );
  const count = client.operationCount();
  expect(() =>
    client.transact(({ local }) => {
      local.payload(a, Uint8Array.of(9));
      expect(() => observers[0].flush()).toThrow('invalid derived value');
      expect(() => local.payload(a, Uint8Array.of(2))).toThrow('failed');
    }),
  ).toThrow('failed');
  expect(values).toEqual([1, 1]);
  expect(rollbacks.every((rollback) => rollback.mock.calls.length === 1)).toBe(true);
  expect(client.operationCount()).toBe(count);
  expect(client.getContent(a)?.payload).toEqual(Uint8Array.of(1));
  client.local.payload(a, Uint8Array.of(2));
  expect(values).toEqual([2, 2]);
});

test('reset scopes roll back after a callback abort; failed bootstrap does not register', () => {
  const { client, native } = open();
  client.local.insert(root, a, null, Uint8Array.of(1));
  let value = 0;
  const observer = client.observeMaterialization(() => {
    const previous = value;
    return {
      apply: () => {
        value = client.getContent(a)!.payload![0];
      },
      rollback: () => {
        value = previous;
      },
    };
  });
  expect(() =>
    client.transact(({ local }) => {
      native.enableReadTracking();
      local.payload(a, Uint8Array.of(2));
      observer.flush();
      expect(value).toBe(2);
      throw new Error('abort reset');
    }),
  ).toThrow('abort reset');
  expect(value).toBe(1);
  const rollback = vi.fn();
  const start = vi.fn(() => ({
    apply: () => {
      throw new Error('invalid bootstrap');
    },
    rollback,
  }));
  expect(() => client.observeMaterialization(start)).toThrow('invalid bootstrap');
  expect(rollback).toHaveBeenCalledTimes(1);
  client.local.payload(a, Uint8Array.of(3));
  expect(start).toHaveBeenCalledTimes(1);
  expect(value).toBe(3);
});

test.each(['write', 'projection', 'observer', 'topology'])(
  'materialization callbacks cannot reenter %s even if caught',
  (action) => {
    const { client } = open();
    client.local.insert(root, a, null, Uint8Array.of(1));
    const projection = client.createProjection(options);
    const observer = client.observeMaterialization(() => ({
      apply: () => {
        if (client.getContent(a)?.payload?.[0] !== 9) return;
        expect(() => {
          if (action === 'write') client.local.payload(a, Uint8Array.of(2));
          else if (action === 'projection') projection.get(a);
          else if (action === 'observer') observer.flush();
          else client.getChildren(root);
        }).toThrow('cannot');
      },
      rollback: () => {},
    }));
    expect(() => client.local.payload(a, Uint8Array.of(9))).toThrow('cannot');
    expect(client.getContent(a)?.payload).toEqual(Uint8Array.of(1));
    expect(() =>
      client.transact(() => client.observeMaterialization(() => ({ apply() {}, rollback() {} }))),
    ).toThrow('during a transaction');
  },
);

test('materialization start and apply callbacks must be synchronous', () => {
  const { client } = open();
  expect(() =>
    client.observeMaterialization(
      // @ts-expect-error Asynchronous start violates the synchronous callback contract.
      async () => ({ apply() {}, rollback() {} }),
    ),
  ).toThrow('synchronous');
  const rollback = vi.fn();
  expect(() => client.observeMaterialization(() => ({ apply: async () => {}, rollback }))).toThrow(
    'synchronous',
  );
  expect(rollback).toHaveBeenCalledTimes(1);
  client.local.insert(root, a);
});

test.each(['start', 'drain'])(
  '%s failure restores earlier scopes and the native transaction',
  (failure) => {
    const { client, native } = open();
    const rollback = vi.fn();
    const observer = client.observeMaterialization(() => ({ apply() {}, rollback }));
    let failStart = false;
    client.observeMaterialization(() => {
      if (failStart) throw new Error('start failed');
      return { apply() {}, rollback() {} };
    });
    failStart = failure === 'start';
    if (failure === 'drain')
      vi.spyOn(native, 'drainReadChanges').mockImplementationOnce(() => {
        throw new Error('drain failed');
      });
    expect(() =>
      client.transact(({ local }) => {
        local.insert(root, a);
        expect(() => observer.flush()).toThrow('drain failed');
      }),
    ).toThrow('failed');
    expect(rollback).toHaveBeenCalledTimes(1);
    expect(client.get(a)).toBeUndefined();
    expect(client.operationCount()).toBe(0);
    failStart = false;
    client.local.insert(root, b);
    expect(client.get(b)).toBeDefined();
  },
);

test.each(['throw', 'async'])(
  'failed %s rollback recovers other scopes and fails closed without preventing cleanup',
  (failure) => {
    const { client, native } = open();
    client.local.insert(root, a, null, Uint8Array.of(1));
    let value = 0;
    const observer = client.observeMaterialization(() => {
      const previous = value;
      return {
        apply: () => {
          value = client.getContent(a)!.payload![0];
        },
        rollback: () => {
          value = previous;
        },
      };
    });
    client.observeMaterialization(() => ({
      apply: () => {},
      rollback:
        failure === 'async'
          ? async () => {}
          : () => {
              throw new Error('broken rollback');
            },
    }));
    expect(() =>
      client.transact(({ local }) => {
        local.payload(a, Uint8Array.of(9));
        observer.flush();
        expect(value).toBe(9);
        throw new Error('abort');
      }),
    ).toThrow('could not restore materialized state');
    expect(value).toBe(1);
    expect(native.readContent(a)?.payload).toEqual(Uint8Array.of(1));
    expect(() => client.local.payload(a, Uint8Array.of(2))).toThrow('could not restore');
    expect(() => client.getContent(a)).toThrow('could not restore');
    expect(() => observer.flush()).toThrow('could not restore');
    const free = vi.spyOn(native, 'free');
    client.close();
    expect(free).toHaveBeenCalledTimes(1);
  },
);
