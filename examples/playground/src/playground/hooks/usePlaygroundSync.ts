import { useEffect, useRef, useState } from 'react';
import type { Operation } from '@treecrdt/interface';
import { bytesToHex } from '@treecrdt/interface/ids';
import { SyncPeer, deriveOpRefV0, type Filter, type SyncAuth } from '@treecrdt/sync-protocol';
import {
  createInboundSync,
  createOutboundSync,
  type InboundSync,
  type OutboundSync,
} from '@treecrdt/sync';
import { createTreecrdtSyncBackendFromClient } from '@treecrdt/sync-sqlite';
import type {
  BroadcastPresenceAckMessageV1,
  BroadcastPresenceMessageV1,
} from '@treecrdt/sync-protocol/browser';
import { createBroadcastPresenceMesh } from '@treecrdt/sync-protocol/browser';
import { treecrdtSyncV0ProtobufCodec } from '@treecrdt/sync-protocol/protobuf';
import type { DuplexTransport } from '@treecrdt/sync-protocol/transport';
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
import { startPlaygroundRemoteSyncSocket } from '../remoteSyncSocket';
import {
  formatSyncError,
  isCapabilityRevokedError,
  isDiscoveryBootstrapUrl,
  isRemotePeerId,
  localOpUploadKey,
  normalizeSyncServerUrl,
  previewDiscoveryHost,
  syncOnceOptionsForPeer,
  syncTimeoutMsForPeer,
  withTimeout,
} from '../syncHelpers';

const RECENT_SYNC_TARGET_MS = 5_000;
const NODE_ID_HEX_RE = /^[0-9a-f]{32}$/i;

function isNodeIdHex(id: string): boolean {
  return NODE_ID_HEX_RE.test(id);
}

function childrenFilter(parentId: string): Filter {
  return { children: { parent: hexToBytes16(parentId) } };
}

function syncFilterLabel(filter: Filter, action = 'sync'): string {
  return 'all' in filter
    ? action
    : `${action}(children ${bytesToHex(filter.children.parent).slice(0, 8)}…)`;
}

async function syncFiltersWithTransport(
  peer: SyncPeer<Operation>,
  peerId: string,
  transport: DuplexTransport<any>,
  filters: readonly Filter[],
  opts: {
    autoSync?: boolean;
    multipleTargets?: boolean;
    codewordsPerMessage?: number;
    label?: string;
  } = {},
) {
  const perPeerTimeoutMs = syncTimeoutMsForPeer(peerId, {
    autoSync: opts.autoSync,
    multipleTargets: opts.multipleTargets,
  });
  const codewordsPerMessage = opts.codewordsPerMessage ?? 2048;
  for (const filter of filters) {
    await withTimeout(
      peer.syncOnce(transport, filter, syncOnceOptionsForPeer(peerId, codewordsPerMessage)),
      perPeerTimeoutMs,
      `${syncFilterLabel(filter, opts.label)} with ${peerId.slice(0, 8)}… timed out`,
    );
  }
}

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
  queueOpsForSync: (ops?: Operation[]) => void;
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
    if (online) void outboundSyncRef.current?.flush();
  }, [online]);

  const autoSyncJoinInitial = useRef(autoSyncJoin).current;

  const broadcastChannelRef = useRef<BroadcastChannel | null>(null);
  const presenceMeshRef = useRef<ReturnType<typeof createBroadcastPresenceMesh<any>> | null>(null);
  const { peers, setMeshPeers, removeMeshPeer, setRemotePeer, resetPeers } =
    usePlaygroundSyncPeers();

  const syncPeerRef = useRef<SyncPeer<Operation> | null>(null);
  const syncConnRef = useRef<Map<string, PlaygroundSyncConnection>>(new Map());
  const outboundSyncRef = useRef<OutboundSync<Operation> | null>(null);
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
    addLivePeer,
    removeLivePeer,
    resetLiveWork,
  } = usePlaygroundLiveSubscriptions({
    syncPeerRef,
    setSyncError,
    authCanSyncAll,
  });
  const autoSyncDoneRef = useRef(false);
  const autoSyncInFlightRef = useRef(false);
  const autoSyncAttemptRef = useRef(0);
  const autoSyncPeerIdRef = useRef<string | null>(null);

  const queueOpsForSync = (ops?: Operation[]) => {
    void syncPeerRef.current?.notifyLocalUpdate(ops);
    outboundSyncRef.current?.queue(ops);
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
    outboundSyncRef.current?.removePeer(peerId);
    removeLivePeer(peerId);

    if (isRemotePeerId(peerId)) setRemotePeer(null);
    else removeMeshPeer(peerId);
  };

  const selectSyncTargetIds = (connections: ReadonlyMap<string, PlaygroundSyncConnection>) => {
    const now = Date.now();
    const recentPeerIds = peers
      .filter((p) => now - p.lastSeen < RECENT_SYNC_TARGET_MS)
      .sort((a, b) => b.lastSeen - a.lastSeen)
      .map((p) => p.id);
    return recentPeerIds.length > 0 ? recentPeerIds : Array.from(connections.keys());
  };

  const syncFiltersWithTargets = async (filters: readonly Filter[], label: string) => {
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
    let manualInboundSync: InboundSync<Operation> | null = null;
    try {
      const targets = selectSyncTargetIds(connections).filter((peerId) => connections.has(peerId));
      manualInboundSync = createInboundSync<Operation>({
        localPeer: peer,
        selectPeers: () => targets,
        runSync: async ({ localPeer, peerId, transport, filter }) => {
          await syncFiltersWithTransport(localPeer, peerId, transport, [filter], {
            multipleTargets: targets.length > 1,
            label,
          });
        },
        onError: ({ peerId, error }) => {
          console.error(`${label} failed for peer`, peerId, error);
          manualInboundSync?.removePeer(peerId);
          if (isCapabilityRevokedError(error)) return;
          dropPeerConnection(peerId);
        },
      });
      for (const [peerId, conn] of connections) {
        manualInboundSync.addPeer(peerId, conn.transport);
      }
      for (const filter of filters) await manualInboundSync.scope(filter).syncOnce();
      await refreshMeta();
    } catch (err) {
      console.error(`${label} failed`, err);
      setSyncError(formatSyncError(err));
    } finally {
      manualInboundSync?.close();
      setSyncBusy(false);
    }
  };

  const handleSync = async (filter: Filter) => {
    await syncFiltersWithTargets([filter], 'Sync');
  };

  const handleScopedSync = async () => {
    const parents = new Set(getLoadedParentIds());
    parents.add(viewRootId);
    if (viewRootId !== ROOT_ID) parents.delete(ROOT_ID);
    const parentIds = Array.from(parents).filter(isNodeIdHex);
    parentIds.sort();

    await syncFiltersWithTargets(parentIds.map(childrenFilter), 'Scoped sync');
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
      if (clean === ROOT_ID || !isNodeIdHex(clean)) return;
    }

    if (autoSyncAttemptRef.current >= 3) return;
    autoSyncAttemptRef.current += 1;
    autoSyncInFlightRef.current = true;

    void (async () => {
      setSyncBusy(true);
      setSyncError(null);
      try {
        const filter: Filter = authCanSyncAll ? { all: {} } : childrenFilter(viewRootId);
        await syncFiltersWithTransport(peer, peerId, conn.transport, [filter], {
          autoSync: true,
          label: 'auto sync',
        });

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

    const outboundSync = createOutboundSync<Operation>({
      localPeer: sharedPeer,
      opKey: localOpUploadKey,
      isOnline: () => onlineRef.current,
      shouldSyncPeer: isRemotePeerId,
      getFallbackFilters: () => {
        const liveChildren = Array.from(liveChildrenParentsRef.current).filter(isNodeIdHex);
        if (liveAllEnabledRef.current || liveChildren.length === 0) return [{ all: {} }];
        return liveChildren.map(childrenFilter);
      },
      runPush: async ({ localPeer, peerId, transport, ops }) => {
        await withTimeout(
          localPeer.pushOps(transport, ops, {
            maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
          }),
          syncTimeoutMsForPeer(peerId, { autoSync: true }),
          `live push with ${peerId.slice(0, 8)}… timed out`,
        );
      },
      runSync: async ({ localPeer, peerId, transport, filter }) => {
        await syncFiltersWithTransport(localPeer, peerId, transport, [filter], {
          autoSync: true,
          codewordsPerMessage: 1024,
          label: 'live sync',
        });
      },
      onWorkStart: beginLiveWork,
      onWorkEnd: endLiveWork,
      onError: ({ peerId, error }) => {
        console.error('Remote live sync failed', error);
        setSyncError(formatSyncError(error));
        if (!isCapabilityRevokedError(error)) dropPeerConnection(peerId);
      },
    });
    outboundSyncRef.current = outboundSync;

    const maybeStartLiveForPeer = (peerId: string) => {
      if (!isRemotePeerId(peerId)) {
        const mesh = presenceMeshRef.current;
        if (!mesh || !mesh.isPeerReady(peerId)) return;
      }
      const conn = connections.get(peerId);
      if (!conn) return;
      addLivePeer(peerId, conn);
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
            outboundSync.addPeer(peerId, transport);
            maybeStartLiveForPeer(peerId);
            if (autoSyncJoinInitial && joinMode && !autoSyncDoneRef.current) {
              autoSyncPeerIdRef.current = peerId;
              bumpAutoSyncJoinTick((t) => t + 1);
            }
            return detach;
          },
          onPeerDisconnected: (peerId) => {
            connections.delete(peerId);
            outboundSync.removePeer(peerId);
            removeLivePeer(peerId);
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

    const stopRemoteSocket =
      remoteSyncUrl.length > 0
        ? startPlaygroundRemoteSyncSocket({
            remoteSyncUrl,
            docId,
            sharedPeer,
            connections,
            outboundSync,
            isCurrent: () => syncConnRef.current === connections,
            setRemoteSyncStatus,
            setSyncError,
            setRemotePeer,
            maybeStartLiveForPeer,
            onAutoSyncPeerReady: (peerId) => {
              if (!autoSyncJoinInitial || !joinMode || autoSyncDoneRef.current) return;
              autoSyncPeerIdRef.current = peerId;
              bumpAutoSyncJoinTick((t) => t + 1);
            },
            dropPeerConnection,
          })
        : undefined;

    return () => {
      if (presenceMeshRef.current === mesh) presenceMeshRef.current = null;
      mesh?.stop();
      stopRemoteSocket?.();
      if (broadcastChannelRef.current === channel) broadcastChannelRef.current = null;
      if (syncPeerRef.current === sharedPeer) syncPeerRef.current = null;
      outboundSync.close();
      if (outboundSyncRef.current === outboundSync) {
        outboundSyncRef.current = null;
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
    queueOpsForSync,
    handleSync,
    handleScopedSync,
    postBroadcastMessage,
  };
}
