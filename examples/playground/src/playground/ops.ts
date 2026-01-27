import type { Operation, OperationKind } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";

function replicaKey(replica: Operation["meta"]["id"]["replica"]): string {
  return typeof replica === "string" ? replica : bytesToHex(replica);
}

export function opKey(op: Operation): string {
  return `${replicaKey(op.meta.id.replica)}\u0000${op.meta.id.counter}`;
}

export function compareOps(a: Operation, b: Operation): number {
  return (
    a.meta.lamport - b.meta.lamport ||
    replicaKey(a.meta.id.replica).localeCompare(replicaKey(b.meta.id.replica)) ||
    a.meta.id.counter - b.meta.id.counter
  );
}

export function mergeSortedOps(prev: Operation[], next: Operation[]): Operation[] {
  if (prev.length === 0) return next.slice();
  if (next.length === 0) return prev;
  if (compareOps(prev[prev.length - 1]!, next[0]!) <= 0) return [...prev, ...next];

  const out = new Array<Operation>(prev.length + next.length);
  let i = 0;
  let j = 0;
  let k = 0;
  while (i < prev.length && j < next.length) {
    if (compareOps(prev[i]!, next[j]!) <= 0) out[k++] = prev[i++]!;
    else out[k++] = next[j++]!;
  }
  while (i < prev.length) out[k++] = prev[i++]!;
  while (j < next.length) out[k++] = next[j++]!;
  return out;
}

export function renderKind(kind: OperationKind): string {
  if (kind.type === "insert") {
    const payloadSuffix = kind.payload !== undefined ? ` (${kind.payload.length} bytes)` : "";
    const key = bytesToHex(kind.orderKey);
    return `insert ${kind.node} under ${kind.parent} key=${key}${payloadSuffix}`;
  }
  if (kind.type === "move") {
    const key = bytesToHex(kind.orderKey);
    return `move ${kind.node} to ${kind.newParent} key=${key}`;
  }
  if (kind.type === "payload") {
    return kind.payload === null
      ? `clear payload ${kind.node}`
      : `set payload ${kind.node} (${kind.payload.length} bytes)`;
  }
  return `${kind.type} ${kind.node}`;
}
