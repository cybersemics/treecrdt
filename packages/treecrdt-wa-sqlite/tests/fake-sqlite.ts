import { vi } from 'vitest';

/** Minimal wa-sqlite WASM module: `cwrap` returns `init`, which tests can assert on. */
export function createFakeModule(initResult = 0) {
  const init = vi.fn(async () => initResult);
  return {
    cwrap: vi.fn(() => init),
    init,
    retryOps: [] as Promise<unknown>[],
    pendingOps: [] as Promise<unknown>[],
  };
}

export type FakeSqliteOptions = {
  failDocId?: string;
  failOpen?: string;
  failOpenError?: Error;
};

/** In-memory wa-sqlite API stand-in for open/init tests (no real WASM). */
export function createFakeSqlite(opts: FakeSqliteOptions = {}) {
  let nextHandle = 1;
  let nextStatement = 100;

  return {
    vfs_register: vi.fn(),
    open_v2: vi.fn(async (filename: string) => {
      if (filename === opts.failOpen) throw opts.failOpenError ?? new Error('OPFS open failed');
      return nextHandle++;
    }),
    statements: vi.fn(() => {
      const statement = nextStatement++;
      return {
        next: async () => ({ value: statement }),
        return: async () => undefined,
      };
    }),
    bind: vi.fn(async (_statement: number, _index: number, value: unknown) => {
      if (value === opts.failDocId) throw new Error('setDocId failed');
    }),
    step: vi.fn(async () => 101),
    column_text: vi.fn(),
    finalize: vi.fn(),
    exec: vi.fn(),
    close: vi.fn(),
  };
}
