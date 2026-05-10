import React, { useEffect, useRef, useState } from 'react';
import { base64urlDecode } from '@treecrdt/auth';
import {
  TreecrdtContentObjectUrlCache,
  browserContentObjectUrlFactory,
  decodeContent,
} from '@treecrdt/content';
import { encryptTreecrdtPayloadV1, maybeDecryptTreecrdtPayloadV1 } from '@treecrdt/crypto';
import type { TreecrdtClient } from '@treecrdt/wa-sqlite/client';

import { loadOrCreateDocPayloadKeyB64 } from '../../auth';
import { ROOT_ID } from '../constants';
import type { PayloadDisplay } from '../types';

type PayloadRecord = {
  payload: Uint8Array | null;
  encrypted?: boolean;
  display: PayloadDisplay;
};

type PayloadPlainRecord = {
  payload: Uint8Array | null;
  encrypted?: boolean;
};

function bytesEqual(left: Uint8Array | null, right: Uint8Array | null): boolean {
  if (left === right) return true;
  if (left === null || right === null) return false;
  if (left.length !== right.length) return false;
  for (let i = 0; i < left.length; i += 1) {
    if (left[i] !== right[i]) return false;
  }
  return true;
}

function payloadRecordsEqual(left: PayloadRecord | undefined, right: PayloadPlainRecord): boolean {
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
  const docPayloadKeyRef = useRef<Uint8Array | null>(null);
  const payloadByNodeRef = useRef<Map<string, PayloadRecord>>(new Map());
  const payloadEventQueueRef = useRef<Promise<void>>(Promise.resolve());
  const imageUrlCacheRef = useRef(
    typeof window === 'undefined'
      ? null
      : (() => {
          const factory = browserContentObjectUrlFactory();
          return factory ? new TreecrdtContentObjectUrlCache(factory) : null;
        })(),
  );

  const resetPayloadCache = React.useCallback(() => {
    payloadEventQueueRef.current = Promise.resolve();
    imageUrlCacheRef.current?.clear();
    payloadByNodeRef.current = new Map();
    setPayloadVersion((v) => v + 1);
  }, []);

  useEffect(() => {
    return () => imageUrlCacheRef.current?.clear();
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

  const decryptPayloadRecord = React.useCallback(
    async (raw: Uint8Array | null): Promise<PayloadPlainRecord> => {
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

  const buildPayloadRecord = React.useCallback(
    (node: string, plain: PayloadPlainRecord): PayloadRecord => {
      const decoded = decodeContent(plain.payload);
      if (decoded.kind === 'empty') {
        imageUrlCacheRef.current?.revoke(node);
        const display: PayloadDisplay = plain.encrypted
          ? { kind: 'encrypted', label: '(encrypted)', value: '' }
          : { kind: 'empty', label: node, value: '' };
        return { ...plain, display };
      }
      if (decoded.kind === 'text') {
        imageUrlCacheRef.current?.revoke(node);
        const label = decoded.text.length === 0 ? '(empty)' : decoded.text;
        return {
          ...plain,
          display: { kind: 'text', label, value: decoded.text },
        };
      }

      const url = imageUrlCacheRef.current?.set(node, decoded) ?? '';
      const label = decoded.name?.trim() || `${decoded.mime} image`;
      return {
        ...plain,
        display: {
          kind: 'image',
          label,
          value: '',
          mime: decoded.mime,
          name: decoded.name,
          size: decoded.size,
          url,
        },
      };
    },
    [],
  );

  const applyPayloadUpdatesFromRaw = React.useCallback(
    async (updates: Iterable<{ node: string; payload: Uint8Array | null }>) => {
      let changed = false;
      const payloads = payloadByNodeRef.current;
      for (const { node, payload } of updates) {
        if (node === ROOT_ID) continue;
        const nextPlain = await decryptPayloadRecord(payload);
        if (payloadRecordsEqual(payloads.get(node), nextPlain)) continue;
        payloads.set(node, buildPayloadRecord(node, nextPlain));
        changed = true;
      }
      if (changed) setPayloadVersion((v) => v + 1);
    },
    [buildPayloadRecord, decryptPayloadRecord],
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
    (updates: Array<{ node: string; payload: Uint8Array | null }>) => {
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
    (id: string): PayloadDisplay => {
      if (id === ROOT_ID) return { kind: 'root', label: 'Root', value: '' };
      const record = payloadByNodeRef.current.get(id);
      return record?.display ?? { kind: 'empty', label: id, value: '' };
    },
    [payloadVersion],
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
