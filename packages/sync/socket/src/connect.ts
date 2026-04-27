import { resolveWebSocketAttachment } from '@treecrdt/discovery';
import { createBrowserWebSocketTransport } from '@treecrdt/sync/browser';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync/protobuf';
import { wrapDuplexTransportWithCodec } from '@treecrdt/sync/transport';
import type { Operation } from '@treecrdt/interface';
import type { SyncMessage } from '@treecrdt/sync';
import { createTreecrdtWebSocketSyncFromTransport } from './create-sync-from-transport.js';
import type {
  ConnectTreecrdtWebSocketSyncOptions,
  CreateTreecrdtWebSocketSyncFromTransportOptions,
  TreecrdtWebSocketSync,
  TreecrdtWebSocketSyncClient,
} from './types.js';
import type { DuplexTransport } from '@treecrdt/sync/transport';

/**
 * Open a binary WebSocket to a sync server, wrap it with the v0 protobuf codec, and return a
 * {@link TreecrdtWebSocketSync} handle. `client.docId` must match the document on the server.
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
  } = options;

  const { url } = await resolveWebSocketAttachment({
    endpoint: baseUrl,
    docId,
    cache: discoveryCache,
    fetch: fetchOverride,
    resolveDocPath,
  });

  if (typeof WebSocket === 'undefined') {
    throw new Error('connectTreecrdtWebSocketSync: WebSocket is not available in this environment');
  }

  const socket = new WebSocket(url.toString());
  socket.binaryType = webSocketBinaryType;

  await new Promise<void>((resolve, reject) => {
    const onError = () => {
      socket.removeEventListener('open', onOpen);
      reject(new Error(`connectTreecrdtWebSocketSync: WebSocket could not open (${url})`));
    };
    const onOpen = () => {
      socket.removeEventListener('error', onError);
      resolve();
    };
    socket.addEventListener('open', onOpen, { once: true });
    socket.addEventListener('error', onError, { once: true });
  });

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
