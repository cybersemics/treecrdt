/// <reference lib="webworker" />
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import {
  transferablesForRpcBinaryResult,
  SHARED_WORKER_DROPPED_ERROR,
  type RpcInitResult,
  type RpcMethod,
  type RpcParams,
  type RpcPushMessage,
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
let finalResetQueued = false;

const settleQueue = <T>(promise: Promise<T>): Promise<void> =>
  promise.then(
    () => undefined,
    () => undefined,
  );

function broadcastMaterialized(event: MaterializationEvent, exclude?: MessagePort) {
  if (event.changes.length === 0) return;
  for (const port of ports) {
    if (port === exclude) continue;
    postToPort(port, { type: 'materialized', event });
  }
}

function postToPort(port: MessagePort, message: unknown, transfer: Transferable[] = []): boolean {
  try {
    port.postMessage(message, transfer);
    return true;
  } catch {
    prunePort(port);
    return false;
  }
}

function scheduleFinalReset(): void {
  if (finalResetQueued || ports.size > 0) return;
  finalResetQueued = true;
  const reset = callQueue.then(async () => {
    finalResetQueued = false;
    if (ports.size === 0) await session.closeDbAndReset();
  });
  callQueue = settleQueue(reset);
}

function prunePort(port: MessagePort): void {
  const removed = detachPort(port);
  port.close();
  if (removed) scheduleFinalReset();
}

function detachPort(port: MessagePort): boolean {
  const removed = ports.delete(port);
  port.onmessage = null;
  port.onmessageerror = null;
  return removed;
}

function invalidatePeers(sourcePort: MessagePort): void {
  const terminal: RpcPushMessage = {
    type: 'terminal',
    error: SHARED_WORKER_DROPPED_ERROR,
  };
  for (const port of ports) {
    if (port === sourcePort) continue;
    if (postToPort(port, terminal)) detachPort(port);
  }
}

(self as unknown as SharedWorkerGlobal).onconnect = (ev: MessageEvent) => {
  const port = ev.ports[0];
  if (!port) return;
  ports.add(port);
  port.onmessage = (message: MessageEvent<RpcRequest | RpcPushMessage>) => {
    const data = message.data;
    if ('type' in data) {
      if (data.type === 'materialized') broadcastMaterialized(data.event, port);
      return;
    }

    const request = data;
    const respondSuccess = (result?: unknown) => {
      const transfer =
        request.method === 'treePayload' || request.method === 'treeParent'
          ? transferablesForRpcBinaryResult(result)
          : [];
      postToPort(port, { id: request.id, ok: true, result }, transfer);
    };
    const respondError = (error: string) => {
      postToPort(port, { id: request.id, ok: false, error });
    };
    let handled = false;
    const run = callQueue.then(() => {
      if (!ports.has(port)) return undefined;
      handled = true;
      return handleRequest(port, request);
    });
    callQueue = settleQueue(run);
    run.then(
      (result) => {
        if (handled) respondSuccess(result);
      },
      (err) => {
        if (handled) respondError(err instanceof Error ? err.message : String(err));
      },
    );
  };
  port.onmessageerror = () => prunePort(port);
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

  if (request.method === 'close') {
    await close(sourcePort);
    return undefined;
  }

  if (request.method === 'drop') {
    await drop(sourcePort);
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
  detachPort(port);
  if (ports.size > 0) return;
  await session.closeDbAndReset();
}

async function drop(sourcePort: MessagePort): Promise<void> {
  try {
    await session.drop();
  } finally {
    invalidatePeers(sourcePort);
    detachPort(sourcePort);
  }
}
