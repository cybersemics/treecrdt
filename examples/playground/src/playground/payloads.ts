import type { Operation } from '@treecrdt/interface';
import { maybeDecryptTreecrdtPayloadV1 } from '@treecrdt/crypto';
import type { TreecrdtClient } from '@treecrdt/wa-sqlite/client';

import { ROOT_ID } from './constants';
import { nodesAffectedByPayloadOps } from './treeState';
import type { PayloadRecord } from './types';

export function applyLocalPayloadPreview(
  payloads: Map<string, PayloadRecord>,
  entries: Iterable<{ nodeId: string; payload: Uint8Array | null }>,
): boolean {
  let changed = false;
  for (const { nodeId, payload } of entries) {
    if (!nodeId || nodeId === ROOT_ID) continue;
    payloads.set(nodeId, { payload, encrypted: false });
    changed = true;
  }
  return changed;
}

export async function hydratePayloadsForOps(opts: {
  payloads: Map<string, PayloadRecord>;
  active: TreecrdtClient;
  ops: Iterable<Operation>;
  docId: string;
  requireDocPayloadKey: () => Promise<Uint8Array>;
  refreshPayloadsForNodes: (active: TreecrdtClient, nodeIds: Iterable<string>) => Promise<void>;
}): Promise<boolean> {
  const { payloads, active, ops, docId, requireDocPayloadKey, refreshPayloadsForNodes } = opts;
  const materialized = Array.isArray(ops) ? ops : Array.from(ops);
  const handled = new Set<string>();
  let changed = false;
  let payloadKeyPromise: Promise<Uint8Array> | null = null;
  const getPayloadKey = () => {
    payloadKeyPromise ??= requireDocPayloadKey();
    return payloadKeyPromise;
  };

  for (const op of materialized) {
    const kind = op.kind;
    if (kind.type === 'insert') {
      if (kind.payload === undefined) {
        payloads.set(kind.node, { payload: null, encrypted: false });
        handled.add(kind.node);
        changed = true;
        continue;
      }
      try {
        const res = await maybeDecryptTreecrdtPayloadV1({
          docId,
          payloadKey: await getPayloadKey(),
          bytes: kind.payload,
        });
        payloads.set(kind.node, { payload: res.plaintext, encrypted: res.encrypted });
      } catch {
        payloads.set(kind.node, { payload: null, encrypted: true });
      }
      handled.add(kind.node);
      changed = true;
      continue;
    }

    if (kind.type === 'payload') {
      if (kind.payload === null) {
        payloads.set(kind.node, { payload: null, encrypted: false });
      } else {
        try {
          const res = await maybeDecryptTreecrdtPayloadV1({
            docId,
            payloadKey: await getPayloadKey(),
            bytes: kind.payload,
          });
          payloads.set(kind.node, { payload: res.plaintext, encrypted: res.encrypted });
        } catch {
          payloads.set(kind.node, { payload: null, encrypted: true });
        }
      }
      handled.add(kind.node);
      changed = true;
    }
  }

  const remaining = [...nodesAffectedByPayloadOps(materialized)].filter((nodeId) => !handled.has(nodeId));
  if (remaining.length > 0) {
    await refreshPayloadsForNodes(active, remaining);
    changed = true;
  }

  return changed;
}
