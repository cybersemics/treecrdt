import type { Operation, SerializeNodeId, SerializeReplica } from "@treecrdt/interface";
import { decodeReplicaId, nodeIdFromBytes16, nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";

import type { NativeOp } from "./native.js";

function assertSafeNonNegativeInteger(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a safe non-negative integer, got: ${value}`);
  }
}

function parseSafeInteger(name: string, value: unknown): number {
  if (typeof value === "number") {
    assertSafeNonNegativeInteger(name, value);
    return value;
  }
  if (typeof value === "bigint") {
    if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error(`${name} out of JS safe range: ${String(value)}`);
    }
    return Number(value);
  }
  if (typeof value === "string") {
    const n = Number(value);
    assertSafeNonNegativeInteger(name, n);
    return n;
  }
  throw new Error(`${name} is not numeric`);
}

export function nativeToOperation(row: NativeOp): Operation {
  const kind = String(row.kind);
  const lamport = parseSafeInteger("lamport", row.lamport);
  const counter = parseSafeInteger("counter", row.counter);
  const replica = decodeReplicaId(row.replica);
  const node = nodeIdFromBytes16(row.node);

  const baseMeta = {
    id: { replica, counter },
    lamport,
    ...(row.knownState != null ? { knownState: row.knownState } : {}),
  };

  switch (kind) {
    case "insert": {
      if (!row.parent) throw new Error("native insert op missing parent");
      if (!row.orderKey) throw new Error("native insert op missing orderKey");
      const parent = nodeIdFromBytes16(row.parent);
      const orderKey = row.orderKey;
      const payload = row.payload;
      return {
        meta: baseMeta,
        kind: {
          type: "insert",
          parent,
          node,
          orderKey,
          ...(payload != null ? { payload } : {}),
        },
      };
    }
    case "move": {
      if (!row.newParent) throw new Error("native move op missing newParent");
      if (!row.orderKey) throw new Error("native move op missing orderKey");
      return {
        meta: baseMeta,
        kind: {
          type: "move",
          node,
          newParent: nodeIdFromBytes16(row.newParent),
          orderKey: row.orderKey,
        },
      };
    }
    case "delete":
      return {
        meta: baseMeta,
        kind: { type: "delete", node },
      };
    case "tombstone":
      return {
        meta: baseMeta,
        kind: { type: "tombstone", node },
      };
    case "payload":
      return {
        meta: baseMeta,
        kind: { type: "payload", node, payload: row.payload ?? null },
      };
    default:
      throw new Error(`unknown operation kind in native: ${kind}`);
  }
}

export function operationToNative(op: Operation): NativeOp {
  return operationToNativeWithSerializers(op, nodeIdToBytes16, replicaIdToBytes);
}

export function operationToNativeWithSerializers(
  op: Operation,
  serializeNodeId: SerializeNodeId,
  serializeReplica: SerializeReplica
): NativeOp {
  assertSafeNonNegativeInteger("lamport", op.meta.lamport);
  assertSafeNonNegativeInteger("counter", op.meta.id.counter);

  const replicaBytes = serializeReplica(op.meta.id.replica);
  const meta = {
    lamport: BigInt(op.meta.lamport),
    replica: Buffer.from(replicaBytes),
    counter: BigInt(op.meta.id.counter),
    ...(op.meta.knownState ? { knownState: Buffer.from(op.meta.knownState) } : {}),
  };

  switch (op.kind.type) {
    case "insert":
      return {
        ...meta,
        kind: "insert",
        parent: Buffer.from(serializeNodeId(op.kind.parent)),
        node: Buffer.from(serializeNodeId(op.kind.node)),
        orderKey: Buffer.from(op.kind.orderKey),
        ...(op.kind.payload !== undefined ? { payload: Buffer.from(op.kind.payload) } : {}),
      };
    case "move":
      return {
        ...meta,
        kind: "move",
        node: Buffer.from(serializeNodeId(op.kind.node)),
        newParent: Buffer.from(serializeNodeId(op.kind.newParent)),
        orderKey: Buffer.from(op.kind.orderKey),
      };
    case "delete":
      return {
        ...meta,
        kind: "delete",
        node: Buffer.from(serializeNodeId(op.kind.node)),
      };
    case "tombstone":
      return {
        ...meta,
        kind: "tombstone",
        node: Buffer.from(serializeNodeId(op.kind.node)),
      };
    case "payload":
      return {
        ...meta,
        kind: "payload",
        node: Buffer.from(serializeNodeId(op.kind.node)),
        ...(op.kind.payload ? { payload: Buffer.from(op.kind.payload) } : {}),
      };
    default: {
      const _exhaustive: never = op.kind;
      throw new Error(`unsupported operation kind: ${String((_exhaustive as any)?.type)}`);
    }
  }
}

export function nativeOpToSqliteRow(row: NativeOp): Record<string, unknown> {
  return {
    lamport: row.lamport,
    replica: row.replica,
    counter: row.counter,
    kind: row.kind,
    parent: row.parent ?? null,
    node: row.node,
    new_parent: row.newParent ?? null,
    order_key: row.orderKey ?? null,
    payload: row.payload ?? null,
    known_state: row.knownState ?? null,
  };
}

