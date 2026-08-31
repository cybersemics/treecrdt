import * as Comlink from 'comlink';
import { vi } from 'vitest';

import type { TreecrdtConnection } from '../src/connection.js';
import type { TreecrdtSession } from '../src/session.js';

export type RejectedMethod = 'init' | 'close' | 'drop';

export type MockConnection = TreecrdtConnection & {
  readonly calls: string[];
};

/**
 * Lifecycle tests never call the data API; Comlink only needs a proxy-marked session.
 */
export function createMockConnection(reject?: RejectedMethod): MockConnection {
  const calls: string[] = [];
  const run = async (method: RejectedMethod) => {
    calls.push(method);
    if (reject === method) throw new Error(`${method} failed`);
  };

  return {
    calls,
    session: Comlink.proxy({} as TreecrdtSession),
    async init() {
      await run('init');
      return { storage: 'memory', filename: ':memory:' };
    },
    close: () => run('close'),
    drop: () => run('drop'),
    subscribeMaterialized() {},
    unsubscribeMaterialized() {},
    async notifyMaterialized() {},
  };
}

/** Client-side port of a MessageChannel with `connection` exposed on the other end. */
function exposeConnection(connection: MockConnection): MessagePort {
  const { port1, port2 } = new MessageChannel();
  Comlink.expose(connection, port1);
  port1.start();
  port2.start();
  return port2;
}

/** Stubs `Worker` so `createTreecrdtClient` talks to `connection` over Comlink. */
export function installDedicatedWorker(connection: MockConnection) {
  let terminated = false;
  let endpoint: MessagePort | null = null;
  vi.stubGlobal(
    'Worker',
    class {
      constructor() {
        const port = exposeConnection(connection);
        endpoint = port;
        return Object.assign(port, {
          terminate() {
            terminated = true;
            port.close();
          },
        });
      }
    },
  );
  return {
    get terminated() {
      return terminated;
    },
    emitError(message = 'worker failed') {
      if (!endpoint) throw new Error('Worker endpoint was not created');
      const event = new Event('error');
      Object.defineProperty(event, 'message', { value: message });
      endpoint.dispatchEvent(event);
    },
    closeEndpoint() {
      endpoint?.close();
    },
  };
}

/** Stubs `SharedWorker` so `createTreecrdtClient` talks to `connection` over Comlink. */
export function installSharedWorker(connection: MockConnection) {
  let closed = false;
  vi.stubGlobal(
    'SharedWorker',
    class {
      port: MessagePort;
      constructor() {
        this.port = exposeConnection(connection);
        const close = this.port.close.bind(this.port);
        this.port.close = () => {
          closed = true;
          close();
        };
      }
    },
  );
  return {
    isClosed: () => closed,
  };
}
