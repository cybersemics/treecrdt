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

/**
 * Resolves when the socket is OPEN. Rejects on `error` or `close` before open (so the handshake
 * cannot hang if no `error` fires). Always removes listeners; on failure, best-effort `close()`.
 */
function waitForWebSocketOpen(socket: WebSocket, url: string | URL): Promise<void> {
  return new Promise((resolve, reject) => {
    const urlStr = String(url);
    let done = false;

    const removeHandshakeListeners = () => {
      socket.removeEventListener('open', handleOpen);
      socket.removeEventListener('error', handleError);
      socket.removeEventListener('close', handleClose);
    };

    const tryCloseSocket = () => {
      try {
        const { readyState } = socket;
        if (readyState !== WebSocket.CLOSED && readyState !== WebSocket.CLOSING) {
          socket.close();
        }
      } catch {
        // ignore
      }
    };

    const rejectAndClose = (message: string) => {
      if (done) return;
      done = true;
      removeHandshakeListeners();
      tryCloseSocket();
      reject(new Error(message));
    };

    const handleOpen = () => {
      if (done) return;
      done = true;
      removeHandshakeListeners();
      resolve();
    };

    const handleError = () => {
      rejectAndClose(`connectTreecrdtWebSocketSync: WebSocket error while connecting to ${urlStr}`);
    };

    const handleClose = (event: Event) => {
      if (done) return;
      const closeEvent = event as CloseEvent;
      const code = typeof closeEvent.code === 'number' ? closeEvent.code : undefined;
      const reasonText = closeEvent.reason ? String(closeEvent.reason).slice(0, 200) : undefined;
      const parts: string[] = [
        `connectTreecrdtWebSocketSync: WebSocket closed before open (${urlStr})`,
      ];
      if (code !== undefined) parts.push(`code=${code}`);
      if (reasonText) parts.push(`reason=${JSON.stringify(reasonText)}`);
      rejectAndClose(parts.join(' '));
    };

    socket.addEventListener('open', handleOpen, { once: true });
    socket.addEventListener('error', handleError, { once: true });
    socket.addEventListener('close', handleClose, { once: true });
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
    onLiveError,
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
    webSocketCtor ??
    (typeof globalThis !== 'undefined'
      ? (globalThis as { WebSocket?: typeof WebSocket }).WebSocket
      : undefined);
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
    onLiveError,
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
