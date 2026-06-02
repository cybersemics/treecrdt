import {
  useEffect,
  useRef,
  useState,
  type Dispatch,
  type MutableRefObject,
  type SetStateAction,
} from 'react';
import type { Operation } from '@treecrdt/interface';
import { createInboundSync, type InboundSync, type InboundSyncOnceOptions } from '@treecrdt/sync';
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

function childrenFilter(parentId: string): Filter {
  return { children: { parent: hexToBytes16(parentId) } };
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

  const liveFilters = () => {
    if (liveAllEnabledRef.current) return [{ all: {} } satisfies Filter];
    return Array.from(liveChildrenParentsRef.current).map(childrenFilter);
  };

  const ensureInboundSync = () => {
    const peer = syncPeerRef.current;
    if (!peer) return null;
    if (inboundSyncRef.current && inboundSyncPeerRef.current === peer) {
      return inboundSyncRef.current;
    }

    inboundSyncRef.current?.close();

    const inbound = createInboundSync<Operation>({
      localPeer: peer,
      syncOptions: (peerId) => syncOnceOptionsForPeer(peerId, 2048),
      subscribeOptions: (peerId) => syncOnceOptionsForPeer(peerId, 1024),
      onStatus: (status) => setLiveBusy(status.busy),
      onError: ({ peerId, filter, error, phase }) => {
        console.error(`Inbound sync(${liveFilterLabel(filter)}) ${phase} failed`, peerId, error);
        setSyncError(formatSyncError(error));
      },
    });
    inboundSyncRef.current = inbound;
    inboundSyncPeerRef.current = peer;
    return inbound;
  };

  const applyLiveScopes = () => {
    ensureInboundSync()?.setLiveScopes(liveFilters());
  };

  const addInboundPeer = (peerId: string, conn: PlaygroundSyncConnection) => {
    const inbound = ensureInboundSync();
    if (!inbound) return;
    inbound.addPeer(peerId, conn.transport as DuplexTransport<SyncMessage<Operation>>);
    inbound.setLiveScopes(liveFilters());
  };

  const removeLivePeer = (peerId: string) => {
    inboundSyncRef.current?.removePeer(peerId);
  };

  const syncInboundOnce = async (
    filters: Filter | readonly Filter[],
    opts?: InboundSyncOnceOptions,
  ) => {
    const inbound = ensureInboundSync();
    if (!inbound) throw new Error('Inbound sync is not ready yet.');
    await inbound.syncOnce(filters, opts);
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
    setLiveBusy(false);
  };

  useEffect(() => {
    liveChildrenParentsRef.current = liveChildrenParents;
    applyLiveScopes();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveChildrenParents]);

  useEffect(() => {
    liveAllEnabledRef.current = liveAllEnabled;
    applyLiveScopes();
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
    addInboundPeer,
    removeLivePeer,
    syncInboundOnce,
    resetLiveWork,
  };
}
