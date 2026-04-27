import { resolveWebSocketAttachment } from '@treecrdt/discovery';
import { createBrowserWebSocketTransport } from '@treecrdt/sync-protocol/browser';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync-protocol/protobuf';
import { wrapDuplexTransportWithCodec } from '@treecrdt/sync-protocol/transport';
import type { Operation } from '@treecrdt/interface';
import type { SyncMessage } from '@treecrdt/sync-protocol';
import { createTreecrdtWebSocketSyncFromTransport } from './create-sync-from-transport.js';
import type {
  ConnectTreecrdtWebSocketSyncOptions,
  CreateTreecrdtWebSocketSyncFromTransportOptions,
  TreecrdtWebSocketSync,
  TreecrdtWebSocketSyncClient,
} from './types.js';
import type { DuplexTransport } from '@treecrdt/sync-protocol/transport';

function waitForWebSocketOpen(socket: WebSocket, url: string | URL): Promise<void> {
  return new Promise((resolve, reject) => {
    const label = String(url);
    const onError = () => {
      reject(new Error(`connectTreecrdtWebSocketSync: WebSocket could not open (${label})`));
    };
    const onOpen = () => {
      resolve();
    };
    socket.addEventListener('open', onOpen, { once: true });
    socket.addEventListener('error', onError, { once: true });
  });
}

/**
 * Open a binary WebSocket to a sync server, wrap it with the v0 protobuf codec, and return a
 * {@link TreecrdtWebSocketSync} handle. `client.docId` must match the document on the server.
 * Uses the global `WebSocket` unless you pass `options.webSocketCtor` (e.g. for Node, use
 * `import { WebSocket } from "undici"`).
 */
export async function connectTreecrdtWebSocketSync(
  client: TreecrdtWebSocketSyncClient,
  options: ConnectTreecrdtWebSocketSyncOptions,
): Promise<TreecrdtWebSocketSync> {
  const docId = client.docId;
  const {
    baseUrl,
    fetch: fetchOverride,
    discoveryCache,
    resolveDocPath,
    enablePendingSidecar,
    auth,
    syncPeerOptions,
    autoNotifyLocalOnWrite,
    webSocketBinaryType = 'arraybuffer',
    webSocketCtor,
  } = options;

  const { url } = await resolveWebSocketAttachment({
    endpoint: baseUrl,
    docId,
    cache: discoveryCache,
    fetch: fetchOverride,
    resolveDocPath,
  });

  const Ctor: typeof WebSocket | undefined =
    webSocketCtor ?? (typeof globalThis !== 'undefined' ? (globalThis as { WebSocket?: typeof WebSocket }).WebSocket : undefined);
  if (!Ctor) {
    throw new Error(
      'connectTreecrdtWebSocketSync: no WebSocket (pass `webSocketCtor`, e.g. `import { WebSocket } from "undici"` in Node)',
    );
  }

  const socket: WebSocket = new Ctor(url);
  socket.binaryType = webSocketBinaryType;

  await waitForWebSocketOpen(socket, url);

  const wire = createBrowserWebSocketTransport(socket);
  const transport: DuplexTransport<SyncMessage<Operation>> = wrapDuplexTransportWithCodec(
    wire,
    treecrdtSyncV0ProtobufCodec,
  );

  const inner: CreateTreecrdtWebSocketSyncFromTransportOptions = {
    enablePendingSidecar,
    auth,
    syncPeerOptions,
    autoNotifyLocalOnWrite,
  };

  return createTreecrdtWebSocketSyncFromTransport(
    client,
    transport,
    () => {
      try {
        wire.close();
      } catch {
        // ignore
      }
    },
    inner,
  );
}
