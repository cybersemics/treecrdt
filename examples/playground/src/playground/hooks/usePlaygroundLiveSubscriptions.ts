import {
  useEffect,
  useRef,
  useState,
  type Dispatch,
  type MutableRefObject,
  type SetStateAction,
} from 'react';
import type { Operation } from '@treecrdt/interface';
import { createInboundSync, type InboundSync, type SyncScope } from '@treecrdt/sync';
import type { Filter, SyncMessage, SyncPeer } from '@treecrdt/sync-protocol';
import type { DuplexTransport } from '@treecrdt/sync-protocol/transport';

import { hexToBytes16 } from '../../sync-v0';
import { formatSyncError, syncOnceOptionsForPeer } from '../syncHelpers';

export type PlaygroundSyncConnection = {
  transport: DuplexTransport<any>;
  detach: () => void;
};

function liveFilterLabel(filter: Filter): string {
  return 'all' in filter ? 'all' : 'children';
}

export function usePlaygroundLiveSubscriptions(opts: {
  syncPeerRef: MutableRefObject<SyncPeer<Operation> | null>;
  setSyncError: Dispatch<SetStateAction<string | null>>;
  authCanSyncAll: boolean;
}) {
  const { syncPeerRef, setSyncError, authCanSyncAll } = opts;
  const [liveBusy, setLiveBusy] = useState(false);
  const [liveChildrenParents, setLiveChildrenParents] = useState<Set<string>>(() => new Set());
  const [liveAllEnabled, setLiveAllEnabled] = useState(false);
  const liveChildrenParentsRef = useRef<Set<string>>(new Set());
  const liveAllEnabledRef = useRef(false);
  const inboundSyncRef = useRef<InboundSync<Operation> | null>(null);
  const inboundSyncPeerRef = useRef<SyncPeer<Operation> | null>(null);
  const liveAllScopeRef = useRef<SyncScope | null>(null);
  const liveChildScopesRef = useRef<Map<string, SyncScope>>(new Map());
  const liveBusyCountRef = useRef(0);

  const beginLiveWork = () => {
    liveBusyCountRef.current += 1;
    setLiveBusy(true);
  };

  const endLiveWork = () => {
    liveBusyCountRef.current = Math.max(0, liveBusyCountRef.current - 1);
    setLiveBusy(liveBusyCountRef.current > 0);
  };

  const ensureInboundSync = () => {
    const peer = syncPeerRef.current;
    if (!peer) return null;
    if (inboundSyncRef.current && inboundSyncPeerRef.current === peer) {
      return inboundSyncRef.current;
    }

    inboundSyncRef.current?.close();
    liveAllScopeRef.current = null;
    liveChildScopesRef.current.clear();

    const inbound = createInboundSync<Operation>({
      localPeer: peer,
      subscribeOptions: (peerId) => syncOnceOptionsForPeer(peerId, 1024),
      onWorkStart: beginLiveWork,
      onWorkEnd: endLiveWork,
      onError: ({ peerId, filter, error, phase }) => {
        console.error(`Live sync(${liveFilterLabel(filter)}) ${phase} failed`, peerId, error);
        setSyncError(formatSyncError(error));
      },
    });
    inboundSyncRef.current = inbound;
    inboundSyncPeerRef.current = peer;
    return inbound;
  };

  const ensureLiveAllScope = () => {
    const inbound = ensureInboundSync();
    if (!inbound) return;
    const scope = liveAllScopeRef.current ?? inbound.scope({ all: {} });
    liveAllScopeRef.current = scope;
    scope.startLive();
  };

  const closeLiveAllScope = () => {
    liveAllScopeRef.current?.close();
    liveAllScopeRef.current = null;
  };

  const ensureLiveChildScope = (parentId: string) => {
    const inbound = ensureInboundSync();
    if (!inbound) return;
    const existing = liveChildScopesRef.current.get(parentId);
    if (existing) {
      existing.startLive();
      return;
    }

    const scope = inbound.scope({ children: { parent: hexToBytes16(parentId) } });
    liveChildScopesRef.current.set(parentId, scope);
    scope.startLive();
  };

  const closeLiveChildScope = (parentId: string) => {
    const scope = liveChildScopesRef.current.get(parentId);
    if (!scope) return;
    scope.close();
    liveChildScopesRef.current.delete(parentId);
  };

  const startDesiredScopes = () => {
    if (liveAllEnabledRef.current) ensureLiveAllScope();
    for (const parentId of liveChildrenParentsRef.current) ensureLiveChildScope(parentId);
  };

  const addLivePeer = (peerId: string, conn: PlaygroundSyncConnection) => {
    const inbound = ensureInboundSync();
    if (!inbound) return;
    inbound.addPeer(peerId, conn.transport as DuplexTransport<SyncMessage<Operation>>);
    startDesiredScopes();
  };

  const removeLivePeer = (peerId: string) => {
    inboundSyncRef.current?.removePeer(peerId);
  };

  const toggleLiveChildren = (parentId: string) => {
    setLiveChildrenParents((prev) => {
      const next = new Set(prev);
      if (next.has(parentId)) next.delete(parentId);
      else next.add(parentId);
      return next;
    });
  };

  const resetLiveWork = () => {
    inboundSyncRef.current?.close();
    inboundSyncRef.current = null;
    inboundSyncPeerRef.current = null;
    liveAllScopeRef.current = null;
    liveChildScopesRef.current.clear();
    liveBusyCountRef.current = 0;
    setLiveBusy(false);
  };

  useEffect(() => {
    liveChildrenParentsRef.current = liveChildrenParents;
    for (const parentId of liveChildrenParents) ensureLiveChildScope(parentId);
    for (const parentId of Array.from(liveChildScopesRef.current.keys())) {
      if (!liveChildrenParents.has(parentId)) closeLiveChildScope(parentId);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveChildrenParents]);

  useEffect(() => {
    liveAllEnabledRef.current = liveAllEnabled;
    if (liveAllEnabled) ensureLiveAllScope();
    else closeLiveAllScope();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveAllEnabled]);

  useEffect(() => {
    if (!authCanSyncAll && liveAllEnabled) setLiveAllEnabled(false);
  }, [authCanSyncAll, liveAllEnabled]);

  return {
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
  };
}
