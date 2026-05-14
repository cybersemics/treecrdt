import {
  useEffect,
  useRef,
  useState,
  type Dispatch,
  type MutableRefObject,
  type SetStateAction,
} from 'react';
import type { Operation } from '@justthrowaway/interface';
import type { SyncPeer, SyncSubscription } from '@justthrowaway/sync-protocol';
import type { DuplexTransport } from '@justthrowaway/sync-protocol/transport';

import { hexToBytes16 } from '../../sync-v0';
import { formatSyncError, syncOnceOptionsForPeer } from '../syncHelpers';

export type PlaygroundSyncConnection = {
  transport: DuplexTransport<any>;
  detach: () => void;
};

export function usePlaygroundLiveSubscriptions(opts: {
  syncPeerRef: MutableRefObject<SyncPeer<Operation> | null>;
  syncConnRef: MutableRefObject<Map<string, PlaygroundSyncConnection>>;
  setSyncError: Dispatch<SetStateAction<string | null>>;
  authCanSyncAll: boolean;
}) {
  const { syncPeerRef, syncConnRef, setSyncError, authCanSyncAll } = opts;
  const [liveBusy, setLiveBusy] = useState(false);
  const [liveChildrenParents, setLiveChildrenParents] = useState<Set<string>>(() => new Set());
  const [liveAllEnabled, setLiveAllEnabled] = useState(false);
  const liveChildrenParentsRef = useRef<Set<string>>(new Set());
  const liveChildSubsRef = useRef<Map<string, Map<string, SyncSubscription>>>(new Map());
  const liveAllEnabledRef = useRef(false);
  const liveAllSubsRef = useRef<Map<string, SyncSubscription>>(new Map());
  const liveAllStartingRef = useRef<Set<string>>(new Set());
  const liveChildrenStartingRef = useRef<Set<string>>(new Set());
  const liveBusyCountRef = useRef(0);

  const beginLiveWork = () => {
    liveBusyCountRef.current += 1;
    setLiveBusy(true);
  };

  const endLiveWork = () => {
    liveBusyCountRef.current = Math.max(0, liveBusyCountRef.current - 1);
    setLiveBusy(liveBusyCountRef.current > 0);
  };

  const stopLiveAllForPeer = (peerId: string) => {
    const existing = liveAllSubsRef.current.get(peerId);
    if (!existing) return;
    existing.stop();
    liveAllSubsRef.current.delete(peerId);
  };

  const stopAllLiveAll = () => {
    for (const sub of liveAllSubsRef.current.values()) sub.stop();
    liveAllSubsRef.current.clear();
  };

  const startLiveAll = (peerId: string) => {
    const conn = syncConnRef.current.get(peerId);
    const peer = syncPeerRef.current;
    if (!conn || !peer) return;

    if (liveAllSubsRef.current.has(peerId)) return;
    if (liveAllStartingRef.current.has(peerId)) return;
    liveAllStartingRef.current.add(peerId);
    beginLiveWork();

    void (async () => {
      let started = false;
      const sub = peer.subscribe(
        conn.transport,
        { all: {} },
        {
          immediate: true,
          intervalMs: 0,
          ...syncOnceOptionsForPeer(peerId, 1024),
        },
      );
      liveAllSubsRef.current.set(peerId, sub);
      void sub.done.catch((err) => {
        if (!started) return;
        console.error('Live sync(all) failed', err);
        stopLiveAllForPeer(peerId);
        setSyncError(formatSyncError(err));
      });

      try {
        await sub.ready;
        started = true;
      } catch (err) {
        console.error('Live sync(all) initial catch-up failed', err);
        stopLiveAllForPeer(peerId);
        setSyncError(formatSyncError(err));
      }
    })().finally(() => {
      liveAllStartingRef.current.delete(peerId);
      endLiveWork();
    });
  };

  const stopLiveChildrenForPeer = (peerId: string) => {
    const byParent = liveChildSubsRef.current.get(peerId);
    if (!byParent) return;
    for (const sub of byParent.values()) sub.stop();
    liveChildSubsRef.current.delete(peerId);
  };

  const stopLiveChildren = (peerId: string, parentId: string) => {
    const byParent = liveChildSubsRef.current.get(peerId);
    if (!byParent) return;
    const sub = byParent.get(parentId);
    if (!sub) return;
    sub.stop();
    byParent.delete(parentId);
    if (byParent.size === 0) liveChildSubsRef.current.delete(peerId);
  };

  const stopAllLiveChildren = () => {
    for (const peerId of Array.from(liveChildSubsRef.current.keys()))
      stopLiveChildrenForPeer(peerId);
  };

  const startLiveChildren = (peerId: string, parentId: string) => {
    const conn = syncConnRef.current.get(peerId);
    const peer = syncPeerRef.current;
    if (!conn || !peer) return;

    const existing = liveChildSubsRef.current.get(peerId);
    if (existing?.has(parentId)) return;
    const startKey = `${peerId}\u0000${parentId}`;
    if (liveChildrenStartingRef.current.has(startKey)) return;
    liveChildrenStartingRef.current.add(startKey);
    beginLiveWork();

    const byParent = existing ?? new Map<string, SyncSubscription>();
    void (async () => {
      let started = false;
      const sub = peer.subscribe(
        conn.transport,
        { children: { parent: hexToBytes16(parentId) } },
        {
          immediate: true,
          intervalMs: 0,
          ...syncOnceOptionsForPeer(peerId, 1024),
        },
      );
      byParent.set(parentId, sub);
      liveChildSubsRef.current.set(peerId, byParent);
      void sub.done.catch((err) => {
        if (!started) return;
        console.error('Live sync failed', err);
        stopLiveChildren(peerId, parentId);
        setSyncError(formatSyncError(err));
      });

      try {
        await sub.ready;
        started = true;
      } catch (err) {
        console.error('Live sync(children) initial catch-up failed', err);
        stopLiveChildren(peerId, parentId);
        setSyncError(formatSyncError(err));
      }
    })().finally(() => {
      liveChildrenStartingRef.current.delete(startKey);
      endLiveWork();
    });
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
    liveAllStartingRef.current.clear();
    liveChildrenStartingRef.current.clear();
    liveBusyCountRef.current = 0;
    setLiveBusy(false);
  };

  useEffect(() => {
    liveChildrenParentsRef.current = liveChildrenParents;

    const connections = syncConnRef.current;
    for (const peerId of connections.keys()) {
      for (const parentId of liveChildrenParents) startLiveChildren(peerId, parentId);
    }

    for (const peerId of Array.from(liveChildSubsRef.current.keys())) {
      if (!connections.has(peerId)) {
        stopLiveChildrenForPeer(peerId);
        continue;
      }
      const byParent = liveChildSubsRef.current.get(peerId);
      if (!byParent) continue;
      for (const parentId of Array.from(byParent.keys())) {
        if (!liveChildrenParents.has(parentId)) stopLiveChildren(peerId, parentId);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveChildrenParents]);

  useEffect(() => {
    liveAllEnabledRef.current = liveAllEnabled;
    const connections = syncConnRef.current;
    if (liveAllEnabled) {
      for (const peerId of connections.keys()) startLiveAll(peerId);
    } else {
      stopAllLiveAll();
    }
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
    startLiveAll,
    stopLiveAllForPeer,
    stopAllLiveAll,
    startLiveChildren,
    stopLiveChildrenForPeer,
    stopAllLiveChildren,
    resetLiveWork,
  };
}
