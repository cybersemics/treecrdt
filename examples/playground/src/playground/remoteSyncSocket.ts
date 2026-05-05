import type { Dispatch, SetStateAction } from 'react';
import type { Operation } from '@treecrdt/interface';
import {
  resolveWebSocketAttachment,
  type ResolveWebSocketAttachmentResult,
} from '@treecrdt/discovery';
import type { OutboundSync } from '@treecrdt/sync';
import type { SyncPeer } from '@treecrdt/sync-protocol';
import { createBrowserWebSocketTransport } from '@treecrdt/sync-protocol/browser';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync-protocol/protobuf';
import {
  wrapDuplexTransportWithCodec,
  type DuplexTransport,
} from '@treecrdt/sync-protocol/transport';

import type { PeerInfo, RemoteSyncStatus } from './types';
import {
  formatRemoteConnectDetail,
  formatRemoteErrorDetail,
  formatRemoteRouteDetail,
  formatSyncError,
  getBrowserDiscoveryRouteCache,
  isDiscoveryBootstrapUrl,
  isTransientRemoteConnectError,
  previewDiscoveryHost,
} from './syncHelpers';

type RemoteSyncConnection = {
  transport: DuplexTransport<any>;
  detach: () => void;
};

export type StartPlaygroundRemoteSyncSocketOptions = {
  remoteSyncUrl: string;
  docId: string;
  sharedPeer: SyncPeer<Operation>;
  connections: Map<string, RemoteSyncConnection>;
  outboundSync: OutboundSync<Operation>;
  isCurrent: () => boolean;
  setRemoteSyncStatus: Dispatch<SetStateAction<RemoteSyncStatus>>;
  setSyncError: Dispatch<SetStateAction<string | null>>;
  setRemotePeer: (peer: PeerInfo | null) => void;
  maybeStartLiveForPeer: (peerId: string) => void;
  onAutoSyncPeerReady: (peerId: string) => void;
  dropPeerConnection: (peerId: string) => void;
};

export function startPlaygroundRemoteSyncSocket(
  opts: StartPlaygroundRemoteSyncSocketOptions,
): () => void {
  let remoteSocket: WebSocket | null = null;
  let remotePeerId: string | null = null;
  let disposed = false;
  let remoteOpened = false;
  let resolvedRemote: ResolveWebSocketAttachmentResult | null = null;
  const discoveryRouteCache = getBrowserDiscoveryRouteCache();
  const isActive = () => !disposed && opts.isCurrent();

  void (async () => {
    try {
      opts.setSyncError((prev) => (isTransientRemoteConnectError(prev) ? null : prev));
      const bootstrapHost = isDiscoveryBootstrapUrl(opts.remoteSyncUrl)
        ? previewDiscoveryHost(opts.remoteSyncUrl)
        : undefined;
      resolvedRemote = await resolveWebSocketAttachment({
        endpoint: opts.remoteSyncUrl,
        docId: opts.docId,
        cache: discoveryRouteCache,
        fetch:
          typeof window !== 'undefined' && typeof window.fetch === 'function'
            ? window.fetch.bind(window)
            : undefined,
      });
      if (!isActive()) return;

      const remoteUrl = resolvedRemote.url;
      const connectVerb =
        resolvedRemote.source === 'network'
          ? 'Resolved attachment, connecting to'
          : resolvedRemote.source === 'cache'
            ? 'Using cached route to'
            : 'Connecting to';
      opts.setRemoteSyncStatus({
        state: 'connecting',
        detail: formatRemoteConnectDetail(connectVerb, remoteUrl.host, bootstrapHost),
      });
      remotePeerId = `remote:${remoteUrl.host}`;
      remoteSocket = new WebSocket(remoteUrl.toString());
      remoteSocket.binaryType = 'arraybuffer';

      remoteSocket.addEventListener('open', () => {
        if (!isActive()) return;
        if (!remoteSocket || remoteSocket.readyState !== WebSocket.OPEN || !remotePeerId) return;
        remoteOpened = true;
        opts.setSyncError((prev) => (isTransientRemoteConnectError(prev) ? null : prev));
        opts.setRemoteSyncStatus({
          detail: formatRemoteRouteDetail(remoteUrl.host, { bootstrapHost }),
          state: 'connected',
        });
        const wire = createBrowserWebSocketTransport(remoteSocket);
        const transport = wrapDuplexTransportWithCodec<Uint8Array, any>(
          wire,
          treecrdtSyncV0ProtobufCodec as any,
        );
        const detach = opts.sharedPeer.attach(transport);
        opts.connections.set(remotePeerId, { transport, detach });
        opts.outboundSync.addPeer(remotePeerId, transport);
        opts.setRemotePeer({ id: remotePeerId, lastSeen: Date.now() });
        opts.maybeStartLiveForPeer(remotePeerId);
        opts.onAutoSyncPeerReady(remotePeerId);
      });

      remoteSocket.addEventListener('message', () => {
        if (!isActive() || !remotePeerId) return;
        opts.setRemotePeer({ id: remotePeerId, lastSeen: Date.now() });
        opts.setRemoteSyncStatus((prev) =>
          prev.state === 'connected'
            ? {
                detail: formatRemoteRouteDetail(remoteUrl.host, { bootstrapHost }),
                state: 'connected',
              }
            : prev,
        );
      });

      remoteSocket.addEventListener('close', () => {
        if (!opts.isCurrent()) return;
        if (!disposed) {
          opts.setRemoteSyncStatus({
            detail: formatRemoteErrorDetail(
              remoteOpened ? 'disconnected' : 'could_not_connect',
              remoteUrl.host,
              bootstrapHost,
            ),
            state: 'error',
          });
        }
        if (!remoteOpened && resolvedRemote?.source === 'cache' && resolvedRemote.cacheKey) {
          void discoveryRouteCache?.delete(resolvedRemote.cacheKey);
        }
        if (remotePeerId) opts.dropPeerConnection(remotePeerId);
      });

      remoteSocket.addEventListener('error', () => {
        if (!opts.isCurrent()) return;
        opts.setRemoteSyncStatus({
          detail: formatRemoteErrorDetail(
            remoteOpened ? 'connection_error' : 'could_not_reach',
            remoteUrl.host,
            bootstrapHost,
          ),
          state: 'error',
        });
        if (!remoteOpened && resolvedRemote?.source === 'cache' && resolvedRemote.cacheKey) {
          void discoveryRouteCache?.delete(resolvedRemote.cacheKey);
        }
        opts.setSyncError((prev) => prev ?? `Remote sync socket error (${remoteUrl.host})`);
      });
    } catch (err) {
      if (!isActive()) return;
      opts.setRemoteSyncStatus({
        state: isDiscoveryBootstrapUrl(opts.remoteSyncUrl) ? 'error' : 'invalid',
        detail: formatSyncError(err),
      });
      opts.setSyncError(formatSyncError(err));
    }
  })();

  return () => {
    disposed = true;
    if (!remoteSocket) return;
    try {
      remoteSocket.close();
    } catch {
      // ignore
    }
  };
}
