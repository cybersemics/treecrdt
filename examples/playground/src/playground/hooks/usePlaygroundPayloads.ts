import React, { useEffect, useMemo, useRef, useState } from 'react';
import { base64urlDecode } from '@treecrdt/auth';
import { encryptTreecrdtPayloadV1, maybeDecryptTreecrdtPayloadV1 } from '@treecrdt/crypto';
import type { TreecrdtClient } from '@treecrdt/wa-sqlite/client';

import { loadOrCreateDocPayloadKeyB64 } from '../../auth';
import { ROOT_ID } from '../constants';
import type { PayloadRecord } from '../types';

export type RawPayloadUpdate = { node: string; payload: Uint8Array | null };

function bytesEqual(left: Uint8Array | null, right: Uint8Array | null): boolean {
  if (left === right) return true;
  if (left === null || right === null) return false;
  if (left.length !== right.length) return false;
  for (let i = 0; i < left.length; i += 1) {
    if (left[i] !== right[i]) return false;
  }
  return true;
}

function payloadRecordsEqual(left: PayloadRecord | undefined, right: PayloadRecord): boolean {
  if (!left) return false;
  return (
    Boolean(left.encrypted) === Boolean(right.encrypted) && bytesEqual(left.payload, right.payload)
  );
}

export function usePlaygroundPayloads(opts: {
  docId: string;
  setError: (message: string) => void;
}) {
  const { docId, setError } = opts;
  const [payloadVersion, setPayloadVersion] = useState(0);
  const textDecoder = useMemo(() => new TextDecoder(), []);
  const docPayloadKeyRef = useRef<Uint8Array | null>(null);
  const payloadByNodeRef = useRef<Map<string, PayloadRecord>>(new Map());
  const payloadEventQueueRef = useRef<Promise<void>>(Promise.resolve());

  const resetPayloadCache = React.useCallback(() => {
    payloadEventQueueRef.current = Promise.resolve();
    payloadByNodeRef.current = new Map();
    setPayloadVersion((v) => v + 1);
  }, []);

  const refreshDocPayloadKey = React.useCallback(async () => {
    const keyB64 = await loadOrCreateDocPayloadKeyB64(docId);
    docPayloadKeyRef.current = base64urlDecode(keyB64);
    return docPayloadKeyRef.current;
  }, [docId]);

  useEffect(() => {
    docPayloadKeyRef.current = null;
    resetPayloadCache();
    let cancelled = false;
    void (async () => {
      try {
        await refreshDocPayloadKey();
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : String(err));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [refreshDocPayloadKey, resetPayloadCache, setError]);

  const requireDocPayloadKey = React.useCallback(async (): Promise<Uint8Array> => {
    if (docPayloadKeyRef.current) return docPayloadKeyRef.current;
    const next = await refreshDocPayloadKey();
    if (!next) throw new Error('doc payload key is missing');
    return next;
  }, [refreshDocPayloadKey]);

  const decodePayloadRecord = React.useCallback(
    async (raw: Uint8Array | null): Promise<PayloadRecord> => {
      if (raw === null) return { payload: null, encrypted: false };
      try {
        const key = await requireDocPayloadKey();
        const res = await maybeDecryptTreecrdtPayloadV1({
          docId,
          payloadKey: key,
          bytes: raw,
        });
        return { payload: res.plaintext, encrypted: res.encrypted };
      } catch {
        return { payload: null, encrypted: true };
      }
    },
    [docId, requireDocPayloadKey],
  );

  const applyPayloadUpdatesFromRaw = React.useCallback(
    async (updates: Iterable<RawPayloadUpdate>) => {
      let changed = false;
      const payloads = payloadByNodeRef.current;
      for (const { node, payload } of updates) {
        if (node === ROOT_ID) continue;
        const next = await decodePayloadRecord(payload);
        if (payloadRecordsEqual(payloads.get(node), next)) continue;
        payloads.set(node, next);
        changed = true;
      }
      if (changed) setPayloadVersion((v) => v + 1);
    },
    [decodePayloadRecord],
  );

  const refreshPayloadsForNodes = React.useCallback(
    async (active: TreecrdtClient, nodeIds: Iterable<string>) => {
      const unique = [...new Set(nodeIds)].filter((id) => id !== ROOT_ID);
      if (unique.length === 0) return;
      const updates = await Promise.all(
        unique.map(async (node) => ({ node, payload: await active.tree.getPayload(node) })),
      );
      await applyPayloadUpdatesFromRaw(updates);
    },
    [applyPayloadUpdatesFromRaw],
  );

  const schedulePayloadEventUpdates = React.useCallback(
    (updates: RawPayloadUpdate[]) => {
      if (updates.length === 0) return;
      const run = payloadEventQueueRef.current
        .catch(() => undefined)
        .then(() => applyPayloadUpdatesFromRaw(updates));
      payloadEventQueueRef.current = run.catch((err) => {
        console.error('Failed to apply payload materialization event', err);
      });
    },
    [applyPayloadUpdatesFromRaw],
  );

  const encryptPayloadBytes = React.useCallback(
    async (payload: Uint8Array | null): Promise<Uint8Array | null> => {
      if (payload === null) return null;
      const key = await requireDocPayloadKey();
      return await encryptTreecrdtPayloadV1({ docId, payloadKey: key, plaintext: payload });
    },
    [docId, requireDocPayloadKey],
  );

  const payloadDisplayForNode = React.useCallback(
    (id: string): { label: string; value: string } => {
      if (id === ROOT_ID) return { label: 'Root', value: '' };
      const record = payloadByNodeRef.current.get(id);
      const payload = record?.payload ?? null;
      if (payload === null) {
        return { label: record?.encrypted ? '(encrypted)' : id, value: '' };
      }
      const value = textDecoder.decode(payload);
      return { label: value.length === 0 ? '(empty)' : value, value };
    },
    [payloadVersion, textDecoder],
  );

  return {
    encryptPayloadBytes,
    payloadDisplayForNode,
    refreshDocPayloadKey,
    refreshPayloadsForNodes,
    resetPayloadCache,
    schedulePayloadEventUpdates,
  };
}
