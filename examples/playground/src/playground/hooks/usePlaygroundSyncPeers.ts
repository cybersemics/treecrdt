import { useCallback, useRef, useState } from 'react';

import type { PeerInfo } from '../types';

export function usePlaygroundSyncPeers() {
  const [peers, setPeers] = useState<PeerInfo[]>([]);
  const meshPeersRef = useRef<PeerInfo[]>([]);
  const remotePeerRef = useRef<PeerInfo | null>(null);

  const publishPeers = useCallback(() => {
    const merged: PeerInfo[] = [...meshPeersRef.current];
    if (remotePeerRef.current) merged.push(remotePeerRef.current);
    merged.sort((a, b) => a.id.localeCompare(b.id));
    setPeers(merged);
  }, []);

  const setMeshPeers = useCallback(
    (next: PeerInfo[]) => {
      meshPeersRef.current = next;
      publishPeers();
    },
    [publishPeers],
  );

  const removeMeshPeer = useCallback(
    (peerId: string) => {
      meshPeersRef.current = meshPeersRef.current.filter((p) => p.id !== peerId);
      publishPeers();
    },
    [publishPeers],
  );

  const setRemotePeer = useCallback(
    (peer: PeerInfo | null) => {
      remotePeerRef.current = peer;
      publishPeers();
    },
    [publishPeers],
  );

  const resetPeers = useCallback(() => {
    meshPeersRef.current = [];
    remotePeerRef.current = null;
    publishPeers();
  }, [publishPeers]);

  return {
    peers,
    setMeshPeers,
    removeMeshPeer,
    setRemotePeer,
    resetPeers,
  };
}
