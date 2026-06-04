/// <reference lib="webworker" />
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import {
  transferablesForRpcBinaryResult,
  type RpcInitResult,
  type RpcMethod,
  type RpcParams,
  type RpcPriority,
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

type ScheduledRequest = {
  priority: RpcPriority | 'normal';
  run: () => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
};

const foregroundQueue: ScheduledRequest[] = [];
const normalQueue: ScheduledRequest[] = [];
const backgroundQueue: ScheduledRequest[] = [];
let requestRunning = false;
let waitingForBackgroundContinuation = false;
let normalDrainScheduled = false;

function scheduleNormalDrain() {
  if (normalDrainScheduled) return;
  normalDrainScheduled = true;
  setTimeout(() => {
    normalDrainScheduled = false;
    waitingForBackgroundContinuation = false;
    drainRequests();
  }, 0);
}

function scheduleRequest(priority: RpcPriority | undefined, run: () => Promise<unknown>) {
  return new Promise<unknown>((resolve, reject) => {
    const request = {
      priority: priority ?? 'normal',
      run,
      resolve,
      reject,
    } satisfies ScheduledRequest;
    if (request.priority === 'background') backgroundQueue.push(request);
    else if (request.priority === 'normal') normalQueue.push(request);
    else foregroundQueue.push(request);
    drainRequests();
  });
}

function drainRequests() {
  if (requestRunning) return;
  const request = foregroundQueue.shift() ?? backgroundQueue.shift();
  // After a background chunk, give the producer one task to enqueue the next chunk before
  // normal writes run. Foreground reads still bypass immediately.
  if (!request && waitingForBackgroundContinuation && normalQueue.length > 0) {
    scheduleNormalDrain();
    return;
  }
  const nextRequest = request ?? normalQueue.shift();
  if (!nextRequest) return;

  requestRunning = true;
  nextRequest
    .run()
    .then(nextRequest.resolve, nextRequest.reject)
    .finally(() => {
      if (nextRequest.priority === 'background') waitingForBackgroundContinuation = true;
      requestRunning = false;
      drainRequests();
    });
}

function broadcastMaterialized(event: MaterializationEvent, exclude?: MessagePort) {
  if (event.changes.length === 0) return;
  for (const port of ports) {
    if (port === exclude) continue;
    port.postMessage({ type: 'materialized', event });
  }
}

function isClientPushMessage(message: RpcRequest | RpcPushMessage): message is RpcPushMessage {
  return 'type' in message && message.type === 'materialized';
}

(self as unknown as SharedWorkerGlobal).onconnect = (ev: MessageEvent) => {
  const port = ev.ports[0];
  if (!port) return;
  ports.add(port);
  port.onmessage = (message: MessageEvent<RpcRequest | RpcPushMessage>) => {
    const data = message.data;
    if (isClientPushMessage(data)) {
      broadcastMaterialized(data.event, port);
      return;
    }

    const request = data;
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
    const run = scheduleRequest(request.priority, () => handleRequest(port, request));
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
