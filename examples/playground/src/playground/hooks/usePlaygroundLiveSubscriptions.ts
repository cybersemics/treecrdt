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

function subscriptionFilterLabel(filter: Filter): string {
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
  const inboundGenerationCounterRef = useRef(0);
  const activeInboundGenerationRef = useRef<number | null>(null);
  const inboundPeerRegistrationsRef = useRef<
    Map<
      string,
      {
        connection: PlaygroundSyncConnection;
        transport: DuplexTransport<SyncMessage<Operation>>;
        unregister: () => void;
      }
    >
  >(new Map());

  const clearInboundPeerRegistrations = () => {
    const registrations = Array.from(inboundPeerRegistrationsRef.current.values());
    inboundPeerRegistrationsRef.current.clear();
    for (const { unregister } of registrations) unregister();
  };

  const currentSubscriptionFilters = () => {
    if (liveAllEnabledRef.current) return [{ all: {} } satisfies Filter];
    return Array.from(liveChildrenParentsRef.current).map(childrenFilter);
  };

  const ensureInboundSync = () => {
    const peer = syncPeerRef.current;
    if (!peer) return null;
    if (inboundSyncRef.current && inboundSyncPeerRef.current === peer) {
      return inboundSyncRef.current;
    }

    activeInboundGenerationRef.current = null;
    clearInboundPeerRegistrations();
    const previousInbound = inboundSyncRef.current;
    inboundSyncRef.current = null;
    inboundSyncPeerRef.current = null;
    if (previousInbound) void previousInbound.close();

    const generation = ++inboundGenerationCounterRef.current;
    activeInboundGenerationRef.current = generation;
    let inbound: InboundSync<Operation> | undefined;
    const isCurrentInbound = () =>
      activeInboundGenerationRef.current === generation &&
      (inbound === undefined || inboundSyncRef.current === inbound);

    inbound = createInboundSync<Operation>({
      localPeer: peer,
      syncOptions: (peerId) => syncOnceOptionsForPeer(peerId, 2048),
      subscribeOptions: (peerId) => syncOnceOptionsForPeer(peerId, 1024),
      onStatus: (status) => {
        if (isCurrentInbound()) setLiveBusy(status.busy);
      },
      onError: ({ peerId, filter, error, phase }) => {
        if (!isCurrentInbound()) return;
        console.error(
          `Inbound sync(${subscriptionFilterLabel(filter)}) ${phase} failed`,
          peerId,
          error,
        );
        setSyncError(formatSyncError(error));
      },
    });
    inboundSyncRef.current = inbound;
    inboundSyncPeerRef.current = peer;
    return inbound;
  };

  const applySubscriptions = () => {
    ensureInboundSync()?.subscribe(currentSubscriptionFilters());
  };

  const addInboundPeer = (peerId: string, conn: PlaygroundSyncConnection) => {
    const inbound = ensureInboundSync();
    if (!inbound) return;
    const transport = conn.transport as DuplexTransport<SyncMessage<Operation>>;
    const previous = inboundPeerRegistrationsRef.current.get(peerId);
    if (previous?.connection !== conn) {
      const registration = {
        connection: conn,
        transport,
        unregister: inbound.addAttachedPeer(peerId, transport),
      };
      inboundPeerRegistrationsRef.current.set(peerId, registration);
      previous?.unregister();
    }
    inbound.subscribe(currentSubscriptionFilters());
  };

  const removeLivePeer = (peerId: string, conn: PlaygroundSyncConnection) => {
    const registration = inboundPeerRegistrationsRef.current.get(peerId);
    if (
      !registration ||
      registration.connection !== conn ||
      registration.transport !== conn.transport
    ) {
      return;
    }
    inboundPeerRegistrationsRef.current.delete(peerId);
    registration.unregister();
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

  const resetLiveWork = (expectedPeer?: SyncPeer<Operation>) => {
    if (expectedPeer && inboundSyncPeerRef.current && inboundSyncPeerRef.current !== expectedPeer) {
      return;
    }
    activeInboundGenerationRef.current = null;
    clearInboundPeerRegistrations();
    const inbound = inboundSyncRef.current;
    inboundSyncRef.current = null;
    inboundSyncPeerRef.current = null;
    if (inbound) void inbound.close();
    setLiveBusy(false);
  };

  useEffect(() => {
    liveChildrenParentsRef.current = liveChildrenParents;
    applySubscriptions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveChildrenParents]);

  useEffect(() => {
    liveAllEnabledRef.current = liveAllEnabled;
    applySubscriptions();
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
