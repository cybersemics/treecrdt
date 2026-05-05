import { useEffect, useRef, useState } from 'react';
import type { Operation } from '@treecrdt/interface';
import { bytesToHex } from '@treecrdt/interface/ids';
import {
  resolveWebSocketAttachment,
  type ResolveWebSocketAttachmentResult,
} from '@treecrdt/discovery';
import { SyncPeer, deriveOpRefV0, type Filter, type SyncAuth } from '@treecrdt/sync-protocol';
import {
  createTreecrdtMultiPeerSyncController,
  type TreecrdtMultiPeerSyncController,
} from '@treecrdt/sync';
import { createTreecrdtSyncBackendFromClient } from '@treecrdt/sync-sqlite';
import type {
  BroadcastPresenceAckMessageV1,
  BroadcastPresenceMessageV1,
} from '@treecrdt/sync-protocol/browser';
import {
  createBroadcastPresenceMesh,
  createBrowserWebSocketTransport,
} from '@treecrdt/sync-protocol/browser';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync-protocol/protobuf';
import {
  wrapDuplexTransportWithCodec,
  type DuplexTransport,
} from '@treecrdt/sync-protocol/transport';
import type { TreecrdtClient } from '@treecrdt/wa-sqlite/client';

import { hexToBytes16, type AuthGrantMessageV1 } from '../../sync-v0';
import {
  PLAYGROUND_PEER_TIMEOUT_MS,
  PLAYGROUND_SYNC_MAX_CODEWORDS,
  PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
  ROOT_ID,
} from '../constants';
import type { PeerInfo, RemoteSyncStatus, SyncTransportMode } from '../types';
import type { StoredAuthMaterial } from '../../auth';
import {
  usePlaygroundLiveSubscriptions,
  type PlaygroundSyncConnection,
} from './usePlaygroundLiveSubscriptions';
import { usePlaygroundSyncPeers } from './usePlaygroundSyncPeers';
import {
  formatRemoteConnectDetail,
  formatRemoteErrorDetail,
  formatRemoteRouteDetail,
  formatSyncError,
  getBrowserDiscoveryRouteCache,
  isCapabilityRevokedError,
  isDiscoveryBootstrapUrl,
  isRemotePeerId,
  isTransientRemoteConnectError,
  localOpUploadKey,
  normalizeSyncServerUrl,
  previewDiscoveryHost,
  syncOnceOptionsForPeer,
  syncTimeoutMsForPeer,
  withTimeout,
} from '../syncHelpers';
type PlaygroundSyncApi = {
  peers: PeerInfo[];
  remoteSyncStatus: RemoteSyncStatus;
  syncBusy: boolean;
  liveBusy: boolean;
  syncError: string | null;
  setSyncError: React.Dispatch<React.SetStateAction<string | null>>;
  liveChildrenParents: Set<string>;
  setLiveChildrenParents: React.Dispatch<React.SetStateAction<Set<string>>>;
  liveAllEnabled: boolean;
  setLiveAllEnabled: React.Dispatch<React.SetStateAction<boolean>>;
  toggleLiveChildren: (parentId: string) => void;
  queueLocalOpsForSync: (ops?: Operation[]) => void;
  handleSync: (filter: Filter) => Promise<void>;
  handleScopedSync: () => Promise<void>;
  postBroadcastMessage: (
    msg: BroadcastPresenceMessageV1 | BroadcastPresenceAckMessageV1 | AuthGrantMessageV1,
  ) => boolean;
};

type UsePlaygroundSyncOptions = {
  client: TreecrdtClient | null;
  status: 'booting' | 'ready' | 'error';
  docId: string;
  selfPeerId: string | null;
  autoSyncJoin?: boolean;
  syncServerUrl?: string;
  transportMode?: SyncTransportMode;
  online: boolean;
  getMaxLamport: () => bigint;
  authEnabled: boolean;
  authMaterial: StoredAuthMaterial;
  syncAuth: SyncAuth<Operation> | null;
  authError: string | null;
  joinMode: boolean;
  authCanSyncAll: boolean;
  viewRootId: string;
  getLoadedParentIds: () => string[];
  refreshMeta: () => Promise<void>;
  onAuthGrantMessage?: (grant: AuthGrantMessageV1) => void;
  onRemoteOpsImported: (ops: Operation[]) => Promise<void> | void;
};

export function usePlaygroundSync(opts: UsePlaygroundSyncOptions): PlaygroundSyncApi {
  const {
    client,
    status,
    docId,
    selfPeerId,
    autoSyncJoin = false,
    syncServerUrl = '',
    transportMode = 'local',
    online,
    getMaxLamport,
    authEnabled,
    authMaterial,
    syncAuth,
    authError,
    joinMode,
    authCanSyncAll,
    viewRootId,
    getLoadedParentIds,
    refreshMeta,
    onAuthGrantMessage,
    onRemoteOpsImported,
  } = opts;

  const [syncBusy, setSyncBusy] = useState(false);
  const [syncError, setSyncError] = useState<string | null>(null);
  const [remoteSyncStatus, setRemoteSyncStatus] = useState<RemoteSyncStatus>({
    state: 'disabled',
    detail: 'Remote server transport is disabled in local tabs mode.',
  });
  const [autoSyncJoinTick, bumpAutoSyncJoinTick] = useState(0);

  const onlineRef = useRef(true);
  useEffect(() => {
    onlineRef.current = online;
    if (online) void remoteSyncControllerRef.current?.flush();
  }, [online]);

  const autoSyncJoinInitial = useRef(autoSyncJoin).current;

  const broadcastChannelRef = useRef<BroadcastChannel | null>(null);
  const presenceMeshRef = useRef<ReturnType<typeof createBroadcastPresenceMesh<any>> | null>(null);
  const { peers, setMeshPeers, removeMeshPeer, setRemotePeer, resetPeers } =
    usePlaygroundSyncPeers();

  const syncPeerRef = useRef<SyncPeer<Operation> | null>(null);
  const syncConnRef = useRef<Map<string, PlaygroundSyncConnection>>(new Map());
  const remoteSyncControllerRef = useRef<TreecrdtMultiPeerSyncController<Operation> | null>(null);
  const {
    liveBusy,
    liveChildrenParents,
    setLiveChildrenParents,
    liveAllEnabled,
    setLiveAllEnabled,
    toggleLiveChildren,
    liveChildrenParentsRef,
    liveAllEnabledRef,
    beginLiveWork,
    endLiveWork,
    startLiveAll,
    stopLiveAllForPeer,
    stopAllLiveAll,
    startLiveChildren,
    stopLiveChildrenForPeer,
    stopAllLiveChildren,
    resetLiveWork,
  } = usePlaygroundLiveSubscriptions({
    syncPeerRef,
    syncConnRef,
    setSyncError,
    authCanSyncAll,
  });
  const autoSyncDoneRef = useRef(false);
  const autoSyncInFlightRef = useRef(false);
  const autoSyncAttemptRef = useRef(0);
  const autoSyncPeerIdRef = useRef<string | null>(null);

  const queueLocalOpsForSync = (ops?: Operation[]) => {
    void syncPeerRef.current?.notifyLocalUpdate(ops);
    remoteSyncControllerRef.current?.queueLocalOps(ops);
  };

  const dropPeerConnection = (peerId: string) => {
    const mesh = presenceMeshRef.current;
    if (mesh && !isRemotePeerId(peerId)) {
      mesh.disconnectPeer(peerId);
      return;
    }

    const connections = syncConnRef.current;
    const conn = connections.get(peerId);
    if (!conn) return;
    try {
      conn.detach();
    } catch {
      // ignore
    }
    try {
      (conn.transport as any).close?.();
    } catch {
      // ignore
    }
    connections.delete(peerId);
    remoteSyncControllerRef.current?.deletePeer(peerId);
    stopLiveAllForPeer(peerId);
    stopLiveChildrenForPeer(peerId);

    if (isRemotePeerId(peerId)) setRemotePeer(null);
    else removeMeshPeer(peerId);
  };

  const handleSync = async (filter: Filter) => {
    if (!onlineRef.current) {
      setSyncError('Offline: toggle Online to sync.');
      return;
    }
    const peer = syncPeerRef.current;
    if (!peer) {
      setSyncError('Sync peer is not ready yet.');
      return;
    }
    const connections = syncConnRef.current;
    if (connections.size === 0) {
      setSyncError('No peers discovered yet.');
      return;
    }

    setSyncBusy(true);
    setSyncError(null);
    try {
      const now = Date.now();
      const recentPeerIds = peers
        .filter((p) => now - p.lastSeen < 5_000)
        .sort((a, b) => b.lastSeen - a.lastSeen)
        .map((p) => p.id);
      const targets = recentPeerIds.length > 0 ? recentPeerIds : Array.from(connections.keys());
      let successes = 0;
      let lastErr: unknown = null;
      for (const peerId of targets) {
        const conn = connections.get(peerId);
        if (!conn) continue;
        const perPeerTimeoutMs = syncTimeoutMsForPeer(peerId, {
          multipleTargets: targets.length > 1,
        });
        try {
          await withTimeout(
            peer.syncOnce(conn.transport, filter, syncOnceOptionsForPeer(peerId, 2048)),
            perPeerTimeoutMs,
            `sync with ${peerId.slice(0, 8)}… timed out`,
          );
          successes += 1;
        } catch (err) {
          lastErr = err;
          console.error('Sync failed for peer', peerId, err);
          if (!isCapabilityRevokedError(err)) dropPeerConnection(peerId);
        }
      }
      if (successes === 0) {
        if (lastErr) throw lastErr;
        throw new Error('No peers responded to sync.');
      }
      await refreshMeta();
    } catch (err) {
      console.error('Sync failed', err);
      setSyncError(formatSyncError(err));
    } finally {
      setSyncBusy(false);
    }
  };

  const handleScopedSync = async () => {
    const parents = new Set(getLoadedParentIds());
    parents.add(viewRootId);
    if (viewRootId !== ROOT_ID) parents.delete(ROOT_ID);
    const parentIds = Array.from(parents).filter((id) => /^[0-9a-f]{32}$/i.test(id));
    parentIds.sort();

    if (!onlineRef.current) {
      setSyncError('Offline: toggle Online to sync.');
      return;
    }
    const peer = syncPeerRef.current;
    if (!peer) {
      setSyncError('Sync peer is not ready yet.');
      return;
    }
    const connections = syncConnRef.current;
    if (connections.size === 0) {
      setSyncError('No peers discovered yet.');
      return;
    }

    setSyncBusy(true);
    setSyncError(null);
    try {
      const now = Date.now();
      const recentPeerIds = peers
        .filter((p) => now - p.lastSeen < 5_000)
        .sort((a, b) => b.lastSeen - a.lastSeen)
        .map((p) => p.id);
      const targets = recentPeerIds.length > 0 ? recentPeerIds : Array.from(connections.keys());
      let successes = 0;
      let lastErr: unknown = null;
      for (const peerId of targets) {
        const conn = connections.get(peerId);
        if (!conn) continue;
        const perPeerTimeoutMs = syncTimeoutMsForPeer(peerId, {
          multipleTargets: targets.length > 1,
        });
        try {
          for (const parentId of parentIds) {
            await withTimeout(
              peer.syncOnce(
                conn.transport,
                { children: { parent: hexToBytes16(parentId) } },
                syncOnceOptionsForPeer(peerId, 2048),
              ),
              perPeerTimeoutMs,
              `sync(children ${parentId.slice(0, 8)}…) with ${peerId.slice(0, 8)}… timed out`,
            );
          }
          successes += 1;
        } catch (err) {
          lastErr = err;
          console.error('Scoped sync failed for peer', peerId, err);
          if (!isCapabilityRevokedError(err)) dropPeerConnection(peerId);
        }
      }
      if (successes === 0) {
        if (lastErr) throw lastErr;
        throw new Error('No peers responded to sync.');
      }
      await refreshMeta();
    } catch (err) {
      console.error('Scoped sync failed', err);
      setSyncError(formatSyncError(err));
    } finally {
      setSyncBusy(false);
    }
  };

  const postBroadcastMessage = (
    msg: BroadcastPresenceMessageV1 | BroadcastPresenceAckMessageV1 | AuthGrantMessageV1,
  ) => {
    const channel = broadcastChannelRef.current;
    if (!channel) return false;
    channel.postMessage(msg);
    return true;
  };

  useEffect(() => {
    if (!autoSyncJoinInitial || !joinMode) return;
    if (autoSyncDoneRef.current) return;
    if (autoSyncInFlightRef.current) return;
    if (!onlineRef.current) return;
    if (syncBusy) return;

    const authReady =
      !authEnabled ||
      (authMaterial.issuerPkB64 &&
        authMaterial.localSkB64 &&
        authMaterial.localPkB64 &&
        authMaterial.localTokensB64.length > 0);
    if (!authReady) return;

    const mesh = presenceMeshRef.current;

    let peerId = autoSyncPeerIdRef.current;
    const connections = syncConnRef.current;
    if (peerId && !connections.has(peerId)) {
      autoSyncPeerIdRef.current = null;
      peerId = null;
    }
    if (!peerId) {
      const candidates = Array.from(connections.keys());
      peerId = candidates.find((id) => !mesh || mesh.isPeerReady(id)) ?? null;
      autoSyncPeerIdRef.current = peerId;
    }
    if (!peerId) return;

    const peer = syncPeerRef.current;
    const conn = connections.get(peerId);
    if (!peer || !conn) return;

    if (!authCanSyncAll) {
      const clean = viewRootId.toLowerCase();
      if (clean === ROOT_ID || !/^[0-9a-f]{32}$/.test(clean)) return;
    }

    if (autoSyncAttemptRef.current >= 3) return;
    autoSyncAttemptRef.current += 1;
    autoSyncInFlightRef.current = true;

    void (async () => {
      setSyncBusy(true);
      setSyncError(null);
      try {
        if (authCanSyncAll) {
          await withTimeout(
            peer.syncOnce(conn.transport, { all: {} }, syncOnceOptionsForPeer(peerId, 2048)),
            syncTimeoutMsForPeer(peerId, { autoSync: true }),
            `auto sync with ${peerId.slice(0, 8)}… timed out`,
          );
        } else {
          await withTimeout(
            peer.syncOnce(
              conn.transport,
              { children: { parent: hexToBytes16(viewRootId) } },
              syncOnceOptionsForPeer(peerId, 2048),
            ),
            syncTimeoutMsForPeer(peerId, { autoSync: true }),
            `auto sync(children ${viewRootId.slice(0, 8)}…) with ${peerId.slice(0, 8)}… timed out`,
          );
        }

        await refreshMeta();

        autoSyncDoneRef.current = true;
        if (typeof window !== 'undefined') {
          const url = new URL(window.location.href);
          url.searchParams.delete('autosync');
          window.history.replaceState({}, '', url);
        }
      } catch (err) {
        console.error('Auto sync failed', err);
        setSyncError(formatSyncError(err));
        autoSyncPeerIdRef.current = null;
        if (!isCapabilityRevokedError(err)) dropPeerConnection(peerId);
      } finally {
        autoSyncInFlightRef.current = false;
        setSyncBusy(false);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    authEnabled,
    authCanSyncAll,
    authMaterial.issuerPkB64,
    authMaterial.localPkB64,
    authMaterial.localSkB64,
    authMaterial.localTokensB64.length,
    autoSyncJoinTick,
    joinMode,
    refreshMeta,
    syncBusy,
    viewRootId,
  ]);

  useEffect(() => {
    if (!client || status !== 'ready') return;
    if (!docId) return;
    const hasBroadcastChannel = typeof BroadcastChannel !== 'undefined';
    const wantsLocalMesh = transportMode !== 'remote';
    const wantsRemoteSocket = transportMode !== 'local';
    const configuredRemoteSyncUrl = syncServerUrl.trim();
    const hasLocalMesh = wantsLocalMesh && hasBroadcastChannel;
    const remoteSyncUrl = wantsRemoteSocket ? configuredRemoteSyncUrl : '';

    if (!wantsRemoteSocket) {
      setRemoteSyncStatus({
        state: 'disabled',
        detail: 'Remote server transport is disabled in local tabs mode.',
      });
    } else if (configuredRemoteSyncUrl.length === 0) {
      setRemoteSyncStatus({
        state: 'missing_url',
        detail: 'Enter either a websocket sync URL or an HTTPS bootstrap URL.',
      });
    } else {
      try {
        if (isDiscoveryBootstrapUrl(configuredRemoteSyncUrl)) {
          setRemoteSyncStatus({
            state: 'connecting',
            detail: `Resolving attachment via ${previewDiscoveryHost(configuredRemoteSyncUrl)}...`,
          });
        } else {
          const remoteUrl = normalizeSyncServerUrl(configuredRemoteSyncUrl, docId);
          setRemoteSyncStatus({
            state: 'connecting',
            detail: `Preparing connection to ${remoteUrl.host}...`,
          });
        }
      } catch (err) {
        setRemoteSyncStatus({
          state: 'invalid',
          detail: formatSyncError(err),
        });
      }
    }

    if (!hasLocalMesh && remoteSyncUrl.length === 0) {
      if (wantsRemoteSocket && configuredRemoteSyncUrl.length === 0) {
        setSyncError('Remote transport requires a sync server URL.');
        return;
      }
      if (wantsLocalMesh && !hasBroadcastChannel) {
        setSyncError('BroadcastChannel is not available in this environment.');
        return;
      }
      setSyncError('No sync transport is configured.');
      return;
    }

    if (!authEnabled) {
      // If auth is off, clear any auth-gating error strings so the UI doesn't keep telling users to import invites.
      setSyncError((prev) =>
        prev && (prev.startsWith('Auth enabled:') || prev.startsWith('Initializing local peer key'))
          ? null
          : prev,
      );
    }

    // Reuse the auth hook's prepared sync auth instead of rebuilding auth from raw material here.
    // `syncAuth` is only published after hello-capability preflight has touched the capability
    // store, which avoids the open-device race where UI tokens exist before auth replay is ready.
    if (authEnabled && !syncAuth) {
      const waitingForInvite = joinMode && authMaterial.localTokensB64.length === 0;
      setSyncError(
        waitingForInvite ? null : (authError ?? 'Auth enabled: initializing keys/tokens...'),
      );
      return;
    }

    if (!selfPeerId) {
      setSyncError('Initializing local peer key...');
      return;
    }

    setSyncError((prev) =>
      prev &&
      (prev.includes('initializing keys/tokens') ||
        prev.startsWith('Initializing local peer key') ||
        prev === 'Remote transport requires a sync server URL.' ||
        prev === 'BroadcastChannel is not available in this environment.' ||
        prev === 'No sync transport is configured.')
        ? null
        : prev,
    );

    const debugSync =
      typeof window !== 'undefined' && new URLSearchParams(window.location.search).has('debugSync');

    const channel = hasLocalMesh ? new BroadcastChannel(`treecrdt-sync-v0:${docId}`) : null;
    broadcastChannelRef.current = channel;
    resetPeers();

    const baseBackend = createTreecrdtSyncBackendFromClient(client, docId, {
      enablePendingSidecar: authEnabled,
      maxLamport: getMaxLamport,
    });
    const backend = {
      ...baseBackend,
      listOpRefs: async (filter: Filter) => {
        const refs = await baseBackend.listOpRefs(filter);
        if (debugSync) {
          const name = 'all' in filter ? 'all' : `children(${bytesToHex(filter.children.parent)})`;
          console.debug(`[sync:${selfPeerId}] listOpRefs(${name}) -> ${refs.length}`);
        }
        return refs;
      },
      applyOps: async (ops: Operation[]) => {
        if (debugSync && ops.length > 0) {
          console.debug(`[sync:${selfPeerId}] applyOps(${ops.length})`);
        }
        if (ops.length > 0) await client.ops.appendMany(ops);
        await onRemoteOpsImported(ops);
      },
    };

    const sharedPeer = new SyncPeer<Operation>(backend, {
      maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
      maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
      deriveOpRef: (op, ctx) =>
        deriveOpRefV0(ctx.docId, { replica: op.meta.id.replica, counter: op.meta.id.counter }),
      ...(syncAuth
        ? {
            auth: syncAuth,
          }
        : {}),
    });
    syncPeerRef.current = sharedPeer;

    const connections = new Map<string, { transport: DuplexTransport<any>; detach: () => void }>();
    syncConnRef.current = connections;

    const remoteSyncController = createTreecrdtMultiPeerSyncController<Operation>({
      peer: sharedPeer,
      opKey: localOpUploadKey,
      isOnline: () => onlineRef.current,
      shouldSyncPeer: isRemotePeerId,
      getFallbackFilters: () => {
        const liveChildren = Array.from(liveChildrenParentsRef.current).filter((id) =>
          /^[0-9a-f]{32}$/i.test(id),
        );
        if (liveAllEnabledRef.current || liveChildren.length === 0) return [{ all: {} }];
        return liveChildren.map((parentId) => ({
          children: { parent: hexToBytes16(parentId) },
        }));
      },
      runPush: async ({ peer, peerId, transport, ops }) => {
        await withTimeout(
          peer.pushOps(transport, ops, {
            maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
          }),
          syncTimeoutMsForPeer(peerId, { autoSync: true }),
          `live push with ${peerId.slice(0, 8)}… timed out`,
        );
      },
      runSync: async ({ peer, peerId, transport, filter }) => {
        await withTimeout(
          peer.syncOnce(transport, filter, syncOnceOptionsForPeer(peerId, 1024)),
          syncTimeoutMsForPeer(peerId, { autoSync: true }),
          `live sync with ${peerId.slice(0, 8)}… timed out`,
        );
      },
      onWorkStart: beginLiveWork,
      onWorkEnd: endLiveWork,
      onError: ({ peerId, error }) => {
        console.error('Remote live sync failed', error);
        setSyncError(formatSyncError(error));
        if (!isCapabilityRevokedError(error)) dropPeerConnection(peerId);
      },
    });
    remoteSyncControllerRef.current = remoteSyncController;

    const maybeStartLiveForPeer = (peerId: string) => {
      if (!isRemotePeerId(peerId)) {
        const mesh = presenceMeshRef.current;
        if (!mesh || !mesh.isPeerReady(peerId)) return;
      }
      if (liveAllEnabledRef.current) startLiveAll(peerId);
      for (const parentId of liveChildrenParentsRef.current) startLiveChildren(peerId, parentId);
    };

    const mesh = channel
      ? createBroadcastPresenceMesh({
          channel,
          selfId: selfPeerId,
          codec: treecrdtSyncV0ProtobufCodec,
          isOnline: () => onlineRef.current,
          peerTimeoutMs: PLAYGROUND_PEER_TIMEOUT_MS,
          onPeersChanged: (next) => {
            setMeshPeers(next.map((p) => ({ id: p.id, lastSeen: p.lastSeen })));
          },
          onPeerReady: (peerId) => {
            maybeStartLiveForPeer(peerId);
            if (autoSyncJoinInitial && joinMode && !autoSyncDoneRef.current) {
              autoSyncPeerIdRef.current = peerId;
              // Ensure the auto-sync effect runs even if peer readiness toggles without changing `peers.length`.
              bumpAutoSyncJoinTick((t) => t + 1);
            }
          },
          onPeerTransport: (peerId, transport) => {
            const detach = sharedPeer.attach(transport);
            connections.set(peerId, { transport, detach });
            remoteSyncController.setPeer(peerId, transport);
            maybeStartLiveForPeer(peerId);
            if (autoSyncJoinInitial && joinMode && !autoSyncDoneRef.current) {
              autoSyncPeerIdRef.current = peerId;
              bumpAutoSyncJoinTick((t) => t + 1);
            }
            return detach;
          },
          onPeerDisconnected: (peerId) => {
            connections.delete(peerId);
            remoteSyncController.deletePeer(peerId);
            stopLiveAllForPeer(peerId);
            stopLiveChildrenForPeer(peerId);
            removeMeshPeer(peerId);
          },
          onBroadcastMessage: (data) => {
            if (!data || typeof data !== 'object') return;
            const msg = data as Partial<AuthGrantMessageV1>;
            if (msg.t !== 'auth_grant_v1') return;

            const grant = msg as Partial<AuthGrantMessageV1>;
            if (typeof grant.doc_id !== 'string') return;
            if (grant.doc_id !== docId) return;
            if (typeof grant.to_replica_pk_hex !== 'string') return;
            if (typeof grant.issuer_pk_b64 !== 'string') return;
            if (typeof grant.token_b64 !== 'string') return;

            const localReplicaHex = selfPeerId;
            if (!localReplicaHex) return;
            if (grant.to_replica_pk_hex.toLowerCase() !== localReplicaHex.toLowerCase()) return;

            onAuthGrantMessage?.(grant as AuthGrantMessageV1);
          },
        })
      : null;

    presenceMeshRef.current = mesh;

    let remoteSocket: WebSocket | null = null;
    let remotePeerId: string | null = null;
    let disposed = false;
    let remoteOpened = false;
    let resolvedRemote: ResolveWebSocketAttachmentResult | null = null;
    const discoveryRouteCache = getBrowserDiscoveryRouteCache();

    if (remoteSyncUrl.length > 0) {
      void (async () => {
        try {
          setSyncError((prev) => (isTransientRemoteConnectError(prev) ? null : prev));
          const bootstrapHost = isDiscoveryBootstrapUrl(remoteSyncUrl)
            ? previewDiscoveryHost(remoteSyncUrl)
            : undefined;
          resolvedRemote = await resolveWebSocketAttachment({
            endpoint: remoteSyncUrl,
            docId,
            cache: discoveryRouteCache,
            fetch:
              typeof window !== 'undefined' && typeof window.fetch === 'function'
                ? window.fetch.bind(window)
                : undefined,
          });
          if (disposed || syncConnRef.current !== connections) return;
          const remoteUrl = resolvedRemote.url;
          const connectVerb =
            resolvedRemote.source === 'network'
              ? 'Resolved attachment, connecting to'
              : resolvedRemote.source === 'cache'
                ? 'Using cached route to'
                : 'Connecting to';
          setRemoteSyncStatus({
            state: 'connecting',
            detail: formatRemoteConnectDetail(connectVerb, remoteUrl.host, bootstrapHost),
          });
          remotePeerId = `remote:${remoteUrl.host}`;
          remoteSocket = new WebSocket(remoteUrl.toString());
          remoteSocket.binaryType = 'arraybuffer';

          remoteSocket.addEventListener('open', () => {
            if (disposed || syncConnRef.current !== connections) return;
            if (!remoteSocket || remoteSocket.readyState !== WebSocket.OPEN || !remotePeerId)
              return;
            remoteOpened = true;
            setSyncError((prev) => (isTransientRemoteConnectError(prev) ? null : prev));
            setRemoteSyncStatus({
              detail: formatRemoteRouteDetail(remoteUrl.host, { bootstrapHost }),
              state: 'connected',
            });
            const wire = createBrowserWebSocketTransport(remoteSocket);
            const transport = wrapDuplexTransportWithCodec<Uint8Array, any>(
              wire,
              treecrdtSyncV0ProtobufCodec as any,
            );
            const detach = sharedPeer.attach(transport);
            syncConnRef.current.set(remotePeerId, { transport, detach });
            remoteSyncController.setPeer(remotePeerId, transport);
            setRemotePeer({ id: remotePeerId, lastSeen: Date.now() });
            maybeStartLiveForPeer(remotePeerId);

            if (autoSyncJoinInitial && joinMode && !autoSyncDoneRef.current) {
              autoSyncPeerIdRef.current = remotePeerId;
              bumpAutoSyncJoinTick((t) => t + 1);
            }
          });

          remoteSocket.addEventListener('message', () => {
            if (disposed || syncConnRef.current !== connections) return;
            if (!remotePeerId) return;
            setRemotePeer({ id: remotePeerId, lastSeen: Date.now() });
            setRemoteSyncStatus((prev) =>
              prev.state === 'connected'
                ? {
                    detail: formatRemoteRouteDetail(remoteUrl.host, { bootstrapHost }),
                    state: 'connected',
                  }
                : prev,
            );
          });

          remoteSocket.addEventListener('close', () => {
            if (syncConnRef.current !== connections) return;
            if (!disposed) {
              setRemoteSyncStatus({
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
            if (!remotePeerId) return;
            dropPeerConnection(remotePeerId);
          });

          remoteSocket.addEventListener('error', () => {
            if (syncConnRef.current !== connections) return;
            setRemoteSyncStatus({
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
            setSyncError((prev) => prev ?? `Remote sync socket error (${remoteUrl.host})`);
          });
        } catch (err) {
          if (disposed || syncConnRef.current !== connections) return;
          setRemoteSyncStatus({
            state: isDiscoveryBootstrapUrl(remoteSyncUrl) ? 'error' : 'invalid',
            detail: formatSyncError(err),
          });
          setSyncError(formatSyncError(err));
        }
      })();
    }

    return () => {
      disposed = true;
      stopAllLiveAll();
      stopAllLiveChildren();
      if (presenceMeshRef.current === mesh) presenceMeshRef.current = null;
      mesh?.stop();
      if (remoteSocket) {
        try {
          remoteSocket.close();
        } catch {
          // ignore
        }
      }
      if (broadcastChannelRef.current === channel) broadcastChannelRef.current = null;
      if (syncPeerRef.current === sharedPeer) syncPeerRef.current = null;
      remoteSyncController.close();
      if (remoteSyncControllerRef.current === remoteSyncController) {
        remoteSyncControllerRef.current = null;
      }
      channel?.close();
      resetLiveWork();
      connections.clear();
      resetPeers();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    authEnabled,
    // Avoid restarting the mesh when `authMaterial` is re-read with the same values.
    syncAuth,
    client,
    docId,
    getMaxLamport,
    joinMode,
    onAuthGrantMessage,
    onRemoteOpsImported,
    selfPeerId,
    syncServerUrl,
    transportMode,
    status,
  ]);

  return {
    peers,
    remoteSyncStatus,
    syncBusy,
    liveBusy,
    syncError,
    setSyncError,
    liveChildrenParents,
    setLiveChildrenParents,
    liveAllEnabled,
    setLiveAllEnabled,
    toggleLiveChildren,
    queueLocalOpsForSync,
    handleSync,
    handleScopedSync,
    postBroadcastMessage,
  };
}
