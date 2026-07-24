import { useEffect, useRef, useState } from 'react';
import type { Operation } from '@treecrdt/interface';
import { bytesToHex } from '@treecrdt/interface/ids';
import {
  resolveWebSocketAttachment,
  type ResolveWebSocketAttachmentResult,
} from '@treecrdt/discovery';
import { SyncPeer, deriveOpRefV0, type Filter, type SyncAuth } from '@treecrdt/sync-protocol';
import { createOutboundSync, type OutboundSync } from '@treecrdt/sync';
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
import type { TreecrdtClient } from '@treecrdt/wa-sqlite';

import { hexToBytes16, type AuthGrantMessageV1 } from '../../sync-v0';
import {
  PLAYGROUND_PEER_TIMEOUT_MS,
  PLAYGROUND_REMOTE_SYNC_TIMEOUT_MS,
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
  areCurrentConnections,
  deleteCurrentConnection,
  formatRemoteConnectDetail,
  formatRemoteErrorDetail,
  formatRemoteRouteDetail,
  formatSyncError,
  getBrowserDiscoveryRouteCache,
  inboundSyncPeerIdsToDrop,
  isCapabilityRevokedError,
  isCurrentConnection,
  isCurrentSyncGeneration,
  isDiscoveryBootstrapUrl,
  isRemotePeerId,
  isTransientRemoteConnectError,
  normalizeSyncServerUrl,
  previewDiscoveryHost,
  runConnectionCleanup,
  syncTimeoutMsForPeer,
} from '../syncHelpers';

const RECENT_SYNC_TARGET_MS = 5_000;
const NODE_ID_HEX_RE = /^[0-9a-f]{32}$/i;

function isNodeIdHex(id: string): boolean {
  return NODE_ID_HEX_RE.test(id);
}

function childrenFilter(parentId: string): Filter {
  return { children: { parent: hexToBytes16(parentId) } };
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
  queueOps: (ops: Operation[]) => void;
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

type SyncRun = {
  generation: number;
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
  const [outboundBusy, setOutboundBusy] = useState(false);
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
  const syncGenerationCounterRef = useRef(0);
  const activeSyncGenerationRef = useRef<number | null>(null);
  const syncRunRef = useRef<SyncRun | null>(null);
  const {
    liveBusy,
    liveChildrenParents,
    setLiveChildrenParents,
    liveAllEnabled,
    setLiveAllEnabled,
    toggleLiveChildren,
    addInboundPeer,
    removeLivePeer,
    syncInboundOnce,
    resetLiveWork,
  } = usePlaygroundLiveSubscriptions({
    syncPeerRef,
    setSyncError,
    authCanSyncAll,
  });
  const autoSyncDoneRef = useRef(false);
  const autoSyncInFlightRef = useRef<SyncRun | null>(null);
  const autoSyncAttemptRef = useRef(0);
  const autoSyncPeerIdRef = useRef<string | null>(null);

  const queueOps = (ops: Operation[]) => {
    outboundSyncRef.current?.queueOps(ops);
  };

  const isCurrentSyncContext = (
    generation: number,
    connections: ReadonlyMap<string, PlaygroundSyncConnection>,
  ) =>
    isCurrentSyncGeneration(activeSyncGenerationRef.current, generation) &&
    syncConnRef.current === connections;

  const retirePeerConnection = (
    generation: number,
    connections: Map<string, PlaygroundSyncConnection>,
    peerId: string,
    connection: PlaygroundSyncConnection,
    opts: { closeTransport?: boolean } = {},
  ): boolean => {
    if (
      !isCurrentSyncContext(generation, connections) ||
      !isCurrentConnection(connections, peerId, connection)
    ) {
      return false;
    }

    try {
      connection.detach();
    } catch {
      // Every cleanup layer still runs before the helper rethrows a cleanup error.
    }
    if (opts.closeTransport) {
      try {
        (connection.transport as any).close?.();
      } catch {
        // ignore
      }
    }

    if (isCurrentSyncContext(generation, connections) && !connections.has(peerId)) {
      if (isRemotePeerId(peerId)) setRemotePeer(null);
      else removeMeshPeer(peerId);
    }
    return true;
  };

  const dropPeerConnection = (
    generation: number,
    connections: Map<string, PlaygroundSyncConnection>,
    peerId: string,
    connection: PlaygroundSyncConnection,
  ): boolean => {
    if (
      !isCurrentSyncContext(generation, connections) ||
      !isCurrentConnection(connections, peerId, connection)
    ) {
      return false;
    }

    const mesh = presenceMeshRef.current;
    if (mesh && !isRemotePeerId(peerId)) {
      mesh.disconnectPeer(peerId);
      if (!isCurrentConnection(connections, peerId, connection)) return true;
    }

    return retirePeerConnection(generation, connections, peerId, connection, {
      closeTransport: true,
    });
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
    const generation = activeSyncGenerationRef.current;
    const peer = syncPeerRef.current;
    const connections = syncConnRef.current;
    if (generation === null || !peer || !isCurrentSyncContext(generation, connections)) {
      setSyncError('Sync peer is not ready yet.');
      return;
    }
    if (connections.size === 0) {
      setSyncError('No peers discovered yet.');
      return;
    }

    const targets = selectSyncTargetIds(connections).filter((peerId) => connections.has(peerId));
    const targetConnections = new Map<string, PlaygroundSyncConnection>();
    for (const peerId of targets) {
      const connection = connections.get(peerId);
      if (connection) targetConnections.set(peerId, connection);
    }

    const run: SyncRun = { generation };
    syncRunRef.current = run;
    setSyncBusy(true);
    setSyncError(null);
    try {
      for (const [peerId, connection] of targetConnections) {
        addInboundPeer(peerId, connection);
      }
      await syncInboundOnce(filters, {
        peerIds: targets,
        syncTimeoutMs: (peerId) =>
          syncTimeoutMsForPeer(peerId, { multipleTargets: targets.length > 1 }),
      });
      if (
        !isCurrentSyncContext(generation, connections) ||
        syncRunRef.current !== run ||
        !areCurrentConnections(connections, targetConnections)
      ) {
        return;
      }
      await refreshMeta();
    } catch (err) {
      if (
        !isCurrentSyncContext(generation, connections) ||
        syncRunRef.current !== run ||
        !areCurrentConnections(connections, targetConnections)
      ) {
        return;
      }
      console.error(`${label} failed`, err);
      for (const peerId of inboundSyncPeerIdsToDrop(err)) {
        const connection = targetConnections.get(peerId);
        if (connection) dropPeerConnection(generation, connections, peerId, connection);
      }
      setSyncError(formatSyncError(err));
    } finally {
      if (syncRunRef.current === run) {
        syncRunRef.current = null;
        if (isCurrentSyncContext(generation, connections)) setSyncBusy(false);
      }
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
    if (syncRunRef.current) return;
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

    const generation = activeSyncGenerationRef.current;
    let peerId = autoSyncPeerIdRef.current;
    const connections = syncConnRef.current;
    if (generation === null || !isCurrentSyncContext(generation, connections)) {
      return;
    }
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

    const conn = connections.get(peerId);
    if (!conn) return;

    if (!authCanSyncAll) {
      const clean = viewRootId.toLowerCase();
      if (clean === ROOT_ID || !isNodeIdHex(clean)) return;
    }

    if (autoSyncAttemptRef.current >= 3) return;
    autoSyncAttemptRef.current += 1;
    const run: SyncRun = { generation };
    syncRunRef.current = run;
    autoSyncInFlightRef.current = run;

    void (async () => {
      setSyncBusy(true);
      setSyncError(null);
      try {
        const filter: Filter = authCanSyncAll ? { all: {} } : childrenFilter(viewRootId);
        addInboundPeer(peerId, conn);
        await syncInboundOnce(filter, {
          peerIds: [peerId],
          syncTimeoutMs: (targetPeerId) => syncTimeoutMsForPeer(targetPeerId, { autoSync: true }),
        });

        if (
          !isCurrentSyncContext(generation, connections) ||
          syncRunRef.current !== run ||
          !isCurrentConnection(connections, peerId, conn)
        ) {
          return;
        }
        await refreshMeta();

        if (
          !isCurrentSyncContext(generation, connections) ||
          syncRunRef.current !== run ||
          !isCurrentConnection(connections, peerId, conn)
        ) {
          return;
        }
        autoSyncDoneRef.current = true;
        if (typeof window !== 'undefined') {
          const url = new URL(window.location.href);
          url.searchParams.delete('autosync');
          window.history.replaceState({}, '', url);
        }
      } catch (err) {
        if (
          !isCurrentSyncContext(generation, connections) ||
          syncRunRef.current !== run ||
          !isCurrentConnection(connections, peerId, conn)
        ) {
          return;
        }
        console.error('Auto sync failed', err);
        setSyncError(formatSyncError(err));
        autoSyncPeerIdRef.current = null;
        for (const failedPeerId of inboundSyncPeerIdsToDrop(err, [peerId])) {
          if (failedPeerId === peerId) {
            dropPeerConnection(generation, connections, peerId, conn);
          }
        }
      } finally {
        if (autoSyncInFlightRef.current === run) autoSyncInFlightRef.current = null;
        if (syncRunRef.current === run) {
          syncRunRef.current = null;
          if (isCurrentSyncContext(generation, connections)) setSyncBusy(false);
        }
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

    const localPeer = new SyncPeer<Operation>(backend, {
      maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
      maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
      deriveOpRef: (op, ctx) =>
        deriveOpRefV0(ctx.docId, {
          replica: op.meta.id.replica,
          counter: op.meta.id.counter,
        }),
      ...(syncAuth
        ? {
            auth: syncAuth,
          }
        : {}),
    });
    syncPeerRef.current = localPeer;

    const connections = new Map<string, PlaygroundSyncConnection>();
    syncConnRef.current = connections;

    const generation = ++syncGenerationCounterRef.current;
    activeSyncGenerationRef.current = generation;
    syncRunRef.current = null;
    autoSyncInFlightRef.current = null;
    setSyncBusy(false);
    setOutboundBusy(false);

    let currentOutboundTarget: {
      peerId: string;
      connection: PlaygroundSyncConnection;
    } | null = null;
    let outboundController: OutboundSync<Operation> | undefined;
    const isCurrentOutboundSync = () =>
      isCurrentSyncContext(generation, connections) &&
      (outboundController === undefined || outboundSyncRef.current === outboundController);

    const outboundSync = createOutboundSync<Operation>({
      isOnline: () => onlineRef.current,
      notifyLocalUpdate: (ops) => localPeer.notifyLocalUpdate(ops),
      pushOptions: {
        maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
      },
      pushTimeoutMs: PLAYGROUND_REMOTE_SYNC_TIMEOUT_MS,
      onStatus: (status) => {
        if (isCurrentOutboundSync()) setOutboundBusy(status.flushing);
      },
      onError: (error) => {
        if (!isCurrentOutboundSync()) return;
        const target = currentOutboundTarget;
        if (!target || !isCurrentConnection(connections, target.peerId, target.connection)) return;
        console.error('Remote op upload failed', error);
        setSyncError(formatSyncError(error));
        if (!isCapabilityRevokedError(error)) {
          dropPeerConnection(generation, connections, target.peerId, target.connection);
        }
      },
    });
    outboundController = outboundSync;
    outboundSyncRef.current = outboundSync;

    const maybeStartLiveForPeer = (peerId: string) => {
      if (!isRemotePeerId(peerId)) {
        const mesh = presenceMeshRef.current;
        if (!mesh || !mesh.isPeerReady(peerId)) return;
      }
      const conn = connections.get(peerId);
      if (!conn) return;
      addInboundPeer(peerId, conn);
    };

    const queueAutoSyncForPeer = (peerId: string) => {
      if (!autoSyncJoinInitial || !joinMode || autoSyncDoneRef.current) return;
      autoSyncPeerIdRef.current = peerId;
      // Ensure the auto-sync effect runs even if peer readiness toggles without changing `peers.length`.
      bumpAutoSyncJoinTick((t) => t + 1);
    };

    const registerPeerConnection = (
      peerId: string,
      transport: DuplexTransport<any>,
      opts: { outbound?: boolean; markRemoteSeen?: boolean } = {},
    ): PlaygroundSyncConnection => {
      const existing = connections.get(peerId);
      if (existing?.transport === transport) return existing;
      if (existing) {
        try {
          existing.detach();
        } catch {
          // Every cleanup layer still ran; continue installing the replacement.
        }
        try {
          (existing.transport as any).close?.();
        } catch {
          // ignore
        }
      }

      const detachPeer = localPeer.attach(transport);
      let detached = false;
      let outboundTarget:
        | {
            peerId: string;
            connection: PlaygroundSyncConnection;
          }
        | undefined;
      let unsetOutboundTarget: (() => void) | undefined;
      const connection: PlaygroundSyncConnection = {
        transport,
        detach: () => {
          if (detached) return;
          detached = true;
          runConnectionCleanup({
            deleteCurrent: () => {
              deleteCurrentConnection(connections, peerId, connection);
            },
            unregisterInbound: () => {
              removeLivePeer(peerId, connection);
            },
            unsetOutbound: () => {
              unsetOutboundTarget?.();
              if (currentOutboundTarget === outboundTarget) currentOutboundTarget = null;
            },
            detachPeer,
          });
        },
      };

      if (opts.outbound) {
        outboundTarget = { peerId, connection };
        unsetOutboundTarget = outboundSync.setTarget((ops, pushOptions) =>
          localPeer.pushOps(transport, ops, pushOptions),
        );
        currentOutboundTarget = outboundTarget;
      }

      connections.set(peerId, connection);
      if (opts.markRemoteSeen) setRemotePeer({ id: peerId, lastSeen: Date.now() });
      maybeStartLiveForPeer(peerId);
      queueAutoSyncForPeer(peerId);
      return connection;
    };

    const mesh = channel
      ? createBroadcastPresenceMesh({
          channel,
          selfId: selfPeerId,
          codec: treecrdtSyncV0ProtobufCodec,
          isOnline: () => onlineRef.current,
          peerTimeoutMs: PLAYGROUND_PEER_TIMEOUT_MS,
          onPeersChanged: (next) => {
            if (!isCurrentSyncContext(generation, connections)) return;
            setMeshPeers(next.map((p) => ({ id: p.id, lastSeen: p.lastSeen })));
          },
          onPeerReady: (peerId) => {
            if (!isCurrentSyncContext(generation, connections)) return;
            maybeStartLiveForPeer(peerId);
            queueAutoSyncForPeer(peerId);
          },
          onPeerTransport: (peerId, transport) => {
            if (!isCurrentSyncContext(generation, connections)) return () => {};
            return registerPeerConnection(peerId, transport).detach;
          },
          onPeerDisconnected: (peerId) => {
            if (isCurrentSyncContext(generation, connections) && !connections.has(peerId)) {
              removeMeshPeer(peerId);
            }
          },
          onBroadcastMessage: (data) => {
            if (!isCurrentSyncContext(generation, connections)) return;
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
    let remoteConnection: PlaygroundSyncConnection | null = null;
    let remoteSocketDisposed = false;
    let remoteOpened = false;
    let resolvedRemote: ResolveWebSocketAttachmentResult | null = null;

    if (remoteSyncUrl.length > 0) {
      const discoveryRouteCache = getBrowserDiscoveryRouteCache();
      const isRemoteSocketCurrent = () =>
        !remoteSocketDisposed && isCurrentSyncContext(generation, connections);

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
          if (!isRemoteSocketCurrent()) return;

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
            if (!isRemoteSocketCurrent()) return;
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
            remoteConnection = registerPeerConnection(remotePeerId, transport, {
              outbound: true,
              markRemoteSeen: true,
            });
          });

          remoteSocket.addEventListener('message', () => {
            if (!isRemoteSocketCurrent() || !remotePeerId) return;
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
            if (!isCurrentSyncContext(generation, connections)) return;
            if (!remoteSocketDisposed) {
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
            if (remotePeerId && remoteConnection) {
              dropPeerConnection(generation, connections, remotePeerId, remoteConnection);
            }
          });

          remoteSocket.addEventListener('error', () => {
            if (!isRemoteSocketCurrent()) return;
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
          if (!isRemoteSocketCurrent()) return;
          setRemoteSyncStatus({
            state: isDiscoveryBootstrapUrl(remoteSyncUrl) ? 'error' : 'invalid',
            detail: formatSyncError(err),
          });
          setSyncError(formatSyncError(err));
        }
      })();
    }

    const stopRemoteSocket = () => {
      remoteSocketDisposed = true;
      if (!remoteSocket) return;
      try {
        remoteSocket.close();
      } catch {
        // ignore
      }
    };

    return () => {
      const wasCurrent = isCurrentSyncContext(generation, connections);
      if (wasCurrent) {
        activeSyncGenerationRef.current = null;
        syncRunRef.current = null;
        autoSyncInFlightRef.current = null;
        setSyncBusy(false);
        setOutboundBusy(false);
      }
      if (presenceMeshRef.current === mesh) presenceMeshRef.current = null;
      resetLiveWork(localPeer);
      for (const conn of connections.values()) {
        try {
          conn.detach();
        } catch {
          // ignore
        }
      }
      connections.clear();
      if (syncConnRef.current === connections) syncConnRef.current = new Map();
      mesh?.stop();
      stopRemoteSocket();
      if (broadcastChannelRef.current === channel) broadcastChannelRef.current = null;
      if (syncPeerRef.current === localPeer) syncPeerRef.current = null;
      if (outboundSyncRef.current === outboundSync) {
        outboundSyncRef.current = null;
      }
      void outboundSync.close();
      channel?.close();
      if (wasCurrent) resetPeers();
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
    liveBusy: liveBusy || outboundBusy,
    syncError,
    setSyncError,
    liveChildrenParents,
    setLiveChildrenParents,
    liveAllEnabled,
    setLiveAllEnabled,
    toggleLiveChildren,
    queueOps,
    handleSync,
    handleScopedSync,
    postBroadcastMessage,
  };
}
