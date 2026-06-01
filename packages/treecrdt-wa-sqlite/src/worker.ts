/// <reference lib="webworker" />
import type { MaterializationEvent } from '@treecrdt/interface/engine';
import { transferablesForRpcBinaryResult, type RpcMethod, type RpcRequest } from './rpc.js';
import { openTreecrdtDb } from './open.js';
import {
  CommonWorkerSession,
  createCommonWorkerRpcHandlers,
  openedToRpcInitResult,
} from './common-worker.js';

const session = new CommonWorkerSession();
const coreHandlers = createCommonWorkerRpcHandlers(session);

function postMaterialized(event: MaterializationEvent) {
  (self as unknown as Worker).postMessage({ type: 'materialized', event });
}

async function init(
  baseUrl: string,
  filename: string | undefined,
  storageParam: 'memory' | 'opfs',
  docId: string,
) {
  await session.closeDbAndReset();
  const opened = await openTreecrdtDb({
    baseUrl,
    filename,
    storage: storageParam,
    docId,
    requireOpfs: false,
    onMaterialized: postMaterialized,
  });
  session.applyOpened(opened);
  return openedToRpcInitResult(opened);
}

async function close() {
  await session.closeDbAndReset();
  return null;
}

const methods = {
  init,
  ...coreHandlers,
  close,
  drop: () => session.drop(),
} as const;

self.onmessage = async (ev: MessageEvent<RpcRequest>) => {
  const { id, method, params } = ev.data;
  const respondSuccess = (result?: unknown) => {
    const transfer =
      method === 'treePayload' || method === 'treeParent'
        ? transferablesForRpcBinaryResult(result)
        : [];
    (self as unknown as Worker).postMessage({ id, ok: true, result }, transfer);
  };
  const respondError = (error: string) => {
    (self as unknown as Worker).postMessage({ id, ok: false, error });
  };

  try {
    const methodFn = (methods as Record<RpcMethod, (...args: any[]) => Promise<any>>)[method];
    if (!methodFn) {
      respondError(`unknown method: ${method}`);
      return;
    }
    const result = await methodFn(...(params ?? []));
    respondSuccess(result);
  } catch (err) {
    respondError(err instanceof Error ? err.message : String(err));
  }
};
