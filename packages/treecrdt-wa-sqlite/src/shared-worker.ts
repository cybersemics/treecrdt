/// <reference lib="webworker" />
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import {
  transferablesForRpcBinaryResult,
  type RpcInitResult,
  type RpcMethod,
  type RpcParams,
  type RpcRequest,
  type RpcResult,
} from './rpc.js';
import { openTreecrdtDb } from './open.js';
import {
  CommonWorkerSession,
  createCommonWorkerRpcHandlers,
  openedToRpcInitResult,
} from './common-worker.js';

type SharedWorkerGlobal = typeof globalThis & {
  onconnect: ((ev: MessageEvent) => void) | null;
};

type StoredConfig = {
  baseUrl: string;
  requestedFilename: string;
  requestedStorage: 'memory' | 'opfs';
  docId: string;
};

class SharedCommonWorkerSession extends CommonWorkerSession {
  storedConfig: StoredConfig | null = null;
  initResult: RpcInitResult | null = null;

  protected onAfterReset(): void {
    this.storedConfig = null;
    this.initResult = null;
  }
}

const ports = new Set<MessagePort>();
const session = new SharedCommonWorkerSession();
const coreHandlers = createCommonWorkerRpcHandlers(session);
let callQueue: Promise<void> = Promise.resolve();

const settleQueue = <T>(promise: Promise<T>): Promise<void> =>
  promise.then(
    () => undefined,
    () => undefined,
  );

function broadcastMaterialized(event: MaterializationEvent, exclude?: MessagePort) {
  if (event.changes.length === 0) return;
  for (const port of ports) {
    if (port === exclude) continue;
    port.postMessage({ type: 'materialized', event });
  }
}

(self as unknown as SharedWorkerGlobal).onconnect = (ev: MessageEvent) => {
  const port = ev.ports[0];
  if (!port) return;
  ports.add(port);
  port.onmessage = (message: MessageEvent<RpcRequest>) => {
    const request = message.data;
    const respondSuccess = (result?: unknown) => {
      const transfer =
        request.method === 'treePayload' || request.method === 'treeParent'
          ? transferablesForRpcBinaryResult(result)
          : [];
      port.postMessage({ id: request.id, ok: true, result }, transfer);
    };
    const respondError = (error: string) => {
      port.postMessage({ id: request.id, ok: false, error });
    };
    const run = callQueue.then(() => handleRequest(port, request));
    callQueue = settleQueue(run);
    run.then(
      (result) => respondSuccess(result),
      (err) => respondError(err instanceof Error ? err.message : String(err)),
    );
  };
  port.start();
};

async function handleRequest<M extends RpcMethod>(
  sourcePort: MessagePort,
  request: RpcRequest<M>,
): Promise<RpcResult<M> | void> {
  if (request.method === 'init') {
    const [baseUrl, filename, storage, docId] = request.params as RpcParams<'init'>;
    return (await init(baseUrl, filename, storage, docId)) as RpcResult<M>;
  }

  if (request.method === 'broadcastMaterialized') {
    const [event] = request.params as RpcParams<'broadcastMaterialized'>;
    broadcastMaterialized(event, sourcePort);
    return undefined;
  }

  if (request.method === 'close') {
    await close(sourcePort);
    return undefined;
  }

  if (request.method === 'drop') {
    await session.drop();
    return undefined;
  }

  const methodFn = coreHandlers[request.method as keyof typeof coreHandlers] as
    | ((...args: any[]) => Promise<unknown>)
    | undefined;
  if (!methodFn) throw new Error(`unknown method: ${request.method}`);
  return (await methodFn(...((request.params ?? []) as any[]))) as RpcResult<M>;
}

async function init(
  baseUrl: string,
  filename: string | undefined,
  storageParam: 'memory' | 'opfs',
  docId: string,
): Promise<RpcInitResult> {
  const requestedFilename = storageParam === 'opfs' ? (filename ?? '/treecrdt.db') : ':memory:';
  if (session.storedConfig && session.initResult) {
    const cfg = session.storedConfig;
    if (
      cfg.baseUrl !== baseUrl ||
      cfg.requestedFilename !== requestedFilename ||
      cfg.requestedStorage !== storageParam ||
      cfg.docId !== docId
    ) {
      throw new Error('shared worker already initialized with a different TreeCRDT database');
    }
    return session.initResult;
  }

  const opened = await openTreecrdtDb({
    baseUrl,
    filename,
    storage: storageParam,
    docId,
    requireOpfs: false,
    opfsVfs: storageParam === 'opfs' ? 'any-context' : undefined,
    onMaterialized: (event) => broadcastMaterialized(event),
  });
  session.applyOpened(opened);
  session.storedConfig = { baseUrl, requestedFilename, requestedStorage: storageParam, docId };
  session.initResult = openedToRpcInitResult(opened);
  return session.initResult;
}

async function close(port: MessagePort) {
  ports.delete(port);
  if (ports.size > 0) return;
  await session.closeDbAndReset();
}
