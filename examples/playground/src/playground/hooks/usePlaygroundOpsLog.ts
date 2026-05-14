import React, { useEffect, useRef, useState } from 'react';
import type { Operation } from '@justthrowaway/interface';
import type { TreecrdtClient } from '@justthrowaway/wa-sqlite/client';

import { compareOps, mergeSortedOps, opKey } from '../ops';
import type { Status } from '../types';

export function usePlaygroundOpsLog(opts: {
  client: TreecrdtClient | null;
  status: Status;
  showOpsPanel: boolean;
  lamportRef: React.MutableRefObject<number>;
  setHeadLamport: React.Dispatch<React.SetStateAction<number>>;
  setError: (message: string) => void;
  refreshPayloadsForNodes: (active: TreecrdtClient, nodeIds: Iterable<string>) => Promise<void>;
}) {
  const {
    client,
    status,
    showOpsPanel,
    lamportRef,
    setHeadLamport,
    setError,
    refreshPayloadsForNodes,
  } = opts;
  const [ops, setOps] = useState<Operation[]>([]);
  const knownOpsRef = useRef<Set<string>>(new Set());
  const showOpsPanelRef = useRef(false);

  const resetOps = React.useCallback(() => {
    setOps([]);
    knownOpsRef.current = new Set();
  }, []);

  useEffect(() => {
    showOpsPanelRef.current = showOpsPanel;
    if (!showOpsPanel) resetOps();
  }, [resetOps, showOpsPanel]);

  const ingestOps = React.useCallback(
    (incoming: Operation[], opts: { assumeSorted?: boolean } = {}) => {
      if (!showOpsPanelRef.current) return;
      if (incoming.length === 0) return;
      const fresh: Operation[] = [];
      const known = knownOpsRef.current;
      for (const op of incoming) {
        const key = opKey(op);
        if (known.has(key)) continue;
        known.add(key);
        fresh.push(op);
      }
      if (fresh.length === 0) return;
      if (!opts.assumeSorted) fresh.sort(compareOps);
      setOps((prev) => mergeSortedOps(prev, fresh));
    },
    [],
  );

  const recordOps = React.useCallback(
    (incoming: Operation[], opts: { assumeSorted?: boolean } = {}) => {
      if (incoming.length === 0) return;
      let nextLamport = lamportRef.current;
      for (const op of incoming) nextLamport = Math.max(nextLamport, op.meta.lamport);
      if (nextLamport !== lamportRef.current) {
        lamportRef.current = nextLamport;
        setHeadLamport(lamportRef.current);
      }
      ingestOps(incoming, opts);
    },
    [ingestOps, lamportRef, setHeadLamport],
  );

  useEffect(() => {
    if (!showOpsPanel) return;
    if (!client || status !== 'ready') return;
    let cancelled = false;
    void (async () => {
      try {
        const fetched = await client.ops.all();
        if (cancelled) return;
        fetched.sort(compareOps);
        setOps(fetched);
        knownOpsRef.current = new Set(fetched.map(opKey));

        const payloadNodeIds = new Set<string>();
        for (const op of fetched) {
          const kind = op.kind;
          if (kind.type === 'insert' || kind.type === 'payload' || kind.type === 'delete') {
            payloadNodeIds.add(kind.node);
          }
        }
        await refreshPayloadsForNodes(client, payloadNodeIds);
      } catch (err) {
        if (cancelled) return;
        console.error('Failed to refresh ops', err);
        setError('Failed to refresh operations (see console)');
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [client, refreshPayloadsForNodes, setError, showOpsPanel, status]);

  return { ops, recordOps, resetOps };
}
