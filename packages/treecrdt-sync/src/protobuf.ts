import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { Operation } from "@treecrdt/interface";
import {
  nodeIdFromBytes16,
  nodeIdToBytes16 as nodeIdToBytes16Impl,
  replicaIdFromBytes,
  replicaIdToBytes,
} from "@treecrdt/interface/ids";

import type { WireCodec } from "./transport.js";
import type {
  Bytes,
  Filter,
  Hello,
  HelloAck,
  OpRef,
  OpsBatch,
  RibltCodewords,
  RibltStatus,
  Subscribe,
  SubscribeAck,
  SyncMessage,
  Unsubscribe,
} from "./types.js";

import { SyncMessageSchema } from "./gen/sync/v0_pb.js";
import {
  CapabilitySchema,
  FilterSpecSchema,
  HelloAckSchema,
  HelloSchema,
  OpsBatchSchema,
  RejectedFilterSchema,
  RibltCodewordSchema,
  RibltCodewordsSchema,
  RibltDecodedSchema,
  RibltFailedSchema,
  RibltFailureReason,
  RibltStatusSchema,
  SubscribeAckSchema,
  SubscribeSchema,
  SyncErrorSchema,
  UnsubscribeSchema,
} from "./gen/sync/v0/messages_pb.js";
import { ChildrenFilterSchema, FilterSchema, FullSyncFilterSchema } from "./gen/sync/v0/filters_pb.js";
import { DeleteOpSchema, InsertOpSchema, MoveOpSchema, OperationSchema, TombstoneOpSchema } from "./gen/sync/v0/ops_pb.js";
import { NodeIdSchema, OpRefSchema, OperationIdSchema, OperationMetadataSchema, ReplicaIdSchema } from "./gen/sync/v0/types_pb.js";

function u64ToNumber(v: bigint, field: string): number {
  if (v > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`${field} too large for number: ${v}`);
  }
  return Number(v);
}

function u64FromNumber(v: number, field: string): bigint {
  if (!Number.isSafeInteger(v) || v < 0) throw new Error(`${field} must be a safe non-negative integer, got: ${v}`);
  return BigInt(v);
}

function toProtoOpRef(opRef: OpRef) {
  return create(OpRefSchema, { bytes: opRef });
}

function fromProtoOpRef(opRef: { bytes: Uint8Array }): OpRef {
  return opRef.bytes;
}

function toProtoFilter(filter: Filter) {
  if ("all" in filter) {
    return create(FilterSchema, { kind: { case: "all", value: create(FullSyncFilterSchema, {}) } });
  }
  return create(FilterSchema, {
    kind: {
      case: "children",
      value: create(ChildrenFilterSchema, { parent: create(NodeIdSchema, { bytes: filter.children.parent }) }),
    },
  });
}

function fromProtoFilter(filter: { kind: { case?: string; value?: any } }): Filter {
  switch (filter.kind.case) {
    case "all":
      return { all: {} };
    case "children": {
      const parent = filter.kind.value?.parent?.bytes as Uint8Array | undefined;
      if (!parent) throw new Error("Filter.children.parent missing");
      return { children: { parent } };
    }
    default:
      throw new Error("Filter: missing kind");
  }
}

function toProtoHello(hello: Hello) {
  const capabilities = hello.capabilities.map((cap) => create(CapabilitySchema, cap));
  const filters = hello.filters.map((spec) =>
    create(FilterSpecSchema, { id: spec.id, filter: toProtoFilter(spec.filter) })
  );
  return create(HelloSchema, { capabilities, filters, maxLamport: hello.maxLamport });
}

function fromProtoHello(hello: any): Hello {
  return {
    capabilities: (hello.capabilities ?? []).map((cap: any) => ({ name: cap.name, value: cap.value })),
    filters: hello.filters.map((spec: any) => {
      if (!spec.filter) throw new Error("Hello.FilterSpec missing filter");
      return { id: spec.id, filter: fromProtoFilter(spec.filter) };
    }),
    maxLamport: hello.maxLamport,
  };
}

function toProtoHelloAck(ack: HelloAck) {
  const capabilities = ack.capabilities.map((cap) => create(CapabilitySchema, cap));

  const rejectedFilters = ack.rejectedFilters.map((rej) =>
    create(RejectedFilterSchema, {
      id: rej.id,
      reason: rej.reason,
      message: rej.message ?? "",
    })
  );

  return create(HelloAckSchema, {
    capabilities,
    acceptedFilters: ack.acceptedFilters,
    rejectedFilters,
    maxLamport: ack.maxLamport,
  });
}

function fromProtoHelloAck(ack: any): HelloAck {
  return {
    capabilities: (ack.capabilities ?? []).map((cap: any) => ({ name: cap.name, value: cap.value })),
    acceptedFilters: ack.acceptedFilters,
    rejectedFilters: (ack.rejectedFilters ?? []).map((rej: any) => ({
      id: rej.id,
      reason: rej.reason,
      message: rej.message.length > 0 ? rej.message : undefined,
    })),
    maxLamport: ack.maxLamport,
  };
}

function toProtoOperation(op: Operation) {
  const meta = create(OperationMetadataSchema, {
    id: create(OperationIdSchema, {
      replica: create(ReplicaIdSchema, { bytes: replicaIdToBytes(op.meta.id.replica) }),
      counter: u64FromNumber(op.meta.id.counter, "OperationId.counter"),
    }),
    lamport: u64FromNumber(op.meta.lamport, "OperationMetadata.lamport"),
  });

  switch (op.kind.type) {
    case "insert":
      return create(OperationSchema, {
        meta,
        kind: {
          case: "insert",
          value: create(InsertOpSchema, {
            parent: create(NodeIdSchema, { bytes: nodeIdToBytes16Impl(op.kind.parent) }),
            node: create(NodeIdSchema, { bytes: nodeIdToBytes16Impl(op.kind.node) }),
            position: op.kind.position,
          }),
        },
      });
    case "move":
      return create(OperationSchema, {
        meta,
        kind: {
          case: "move",
          value: create(MoveOpSchema, {
            node: create(NodeIdSchema, { bytes: nodeIdToBytes16Impl(op.kind.node) }),
            newParent: create(NodeIdSchema, { bytes: nodeIdToBytes16Impl(op.kind.newParent) }),
            position: op.kind.position,
          }),
        },
      });
    case "delete":
      return create(OperationSchema, {
        meta,
        kind: {
          case: "delete",
          value: create(DeleteOpSchema, { node: create(NodeIdSchema, { bytes: nodeIdToBytes16Impl(op.kind.node) }) }),
        },
      });
    case "tombstone":
      return create(OperationSchema, {
        meta,
        kind: {
          case: "tombstone",
          value: create(TombstoneOpSchema, { node: create(NodeIdSchema, { bytes: nodeIdToBytes16Impl(op.kind.node) }) }),
        },
      });
    default: {
      const _exhaustive: never = op.kind;
      throw new Error(`unknown op kind: ${(op.kind as any)?.type ?? _exhaustive}`);
    }
  }
}

function fromProtoOperation(op: any): Operation {
  const meta = op.meta;
  const id = meta?.id;
  const replica = id?.replica?.bytes as Uint8Array | undefined;
  if (!replica) throw new Error("Operation.meta.id.replica missing");

  const counter = id?.counter as bigint | undefined;
  const lamport = meta?.lamport as bigint | undefined;
  if (counter === undefined) throw new Error("Operation.meta.id.counter missing");
  if (lamport === undefined) throw new Error("Operation.meta.lamport missing");

  const outMeta = {
    id: { replica: replicaIdFromBytes(replica), counter: u64ToNumber(counter, "OperationId.counter") },
    lamport: u64ToNumber(lamport, "OperationMetadata.lamport"),
  };

  switch (op.kind.case) {
    case "insert": {
      const parentBytes = op.kind.value?.parent?.bytes as Uint8Array | undefined;
      const nodeBytes = op.kind.value?.node?.bytes as Uint8Array | undefined;
      if (!parentBytes || !nodeBytes) throw new Error("InsertOp missing node ids");
      return {
        meta: outMeta,
        kind: {
          type: "insert",
          parent: nodeIdFromBytes16(parentBytes),
          node: nodeIdFromBytes16(nodeBytes),
          position: op.kind.value.position ?? 0,
        },
      };
    }
    case "move": {
      const nodeBytes = op.kind.value?.node?.bytes as Uint8Array | undefined;
      const parentBytes = op.kind.value?.newParent?.bytes as Uint8Array | undefined;
      if (!nodeBytes || !parentBytes) throw new Error("MoveOp missing node ids");
      return {
        meta: outMeta,
        kind: {
          type: "move",
          node: nodeIdFromBytes16(nodeBytes),
          newParent: nodeIdFromBytes16(parentBytes),
          position: op.kind.value.position ?? 0,
        },
      };
    }
    case "delete": {
      const nodeBytes = op.kind.value?.node?.bytes as Uint8Array | undefined;
      if (!nodeBytes) throw new Error("DeleteOp missing node id");
      return { meta: outMeta, kind: { type: "delete", node: nodeIdFromBytes16(nodeBytes) } };
    }
    case "tombstone": {
      const nodeBytes = op.kind.value?.node?.bytes as Uint8Array | undefined;
      if (!nodeBytes) throw new Error("TombstoneOp missing node id");
      return { meta: outMeta, kind: { type: "tombstone", node: nodeIdFromBytes16(nodeBytes) } };
    }
    default:
      throw new Error("Operation: missing kind");
  }
}

function toProtoOpsBatch(batch: OpsBatch<Operation>) {
  return create(OpsBatchSchema, {
    filterId: batch.filterId,
    ops: batch.ops.map(toProtoOperation),
    done: batch.done,
  });
}

function fromProtoOpsBatch(batch: any): OpsBatch<Operation> {
  return {
    filterId: batch.filterId,
    ops: (batch.ops ?? []).map(fromProtoOperation),
    done: !!batch.done,
  };
}

function toProtoSubscribe(sub: Subscribe) {
  return create(SubscribeSchema, {
    subscriptionId: sub.subscriptionId,
    filter: toProtoFilter(sub.filter),
  });
}

function fromProtoSubscribe(sub: any): Subscribe {
  if (!sub.filter) throw new Error("Subscribe.filter missing");
  return {
    subscriptionId: sub.subscriptionId,
    filter: fromProtoFilter(sub.filter),
  };
}

function toProtoSubscribeAck(ack: SubscribeAck) {
  return create(SubscribeAckSchema, {
    subscriptionId: ack.subscriptionId,
    currentLamport: ack.currentLamport,
  });
}

function fromProtoSubscribeAck(ack: any): SubscribeAck {
  return {
    subscriptionId: ack.subscriptionId,
    currentLamport: ack.currentLamport,
  };
}

function toProtoUnsubscribe(msg: Unsubscribe) {
  return create(UnsubscribeSchema, { subscriptionId: msg.subscriptionId });
}

function fromProtoUnsubscribe(msg: any): Unsubscribe {
  return { subscriptionId: msg.subscriptionId };
}

function toProtoRibltCodewords(msg: RibltCodewords) {
  return create(RibltCodewordsSchema, {
    filterId: msg.filterId,
    round: msg.round,
    startIndex: msg.startIndex,
    codewords: msg.codewords.map((cw) =>
      create(RibltCodewordSchema, { count: cw.count, keySum: cw.keySum, valueSum: cw.valueSum })
    ),
  });
}

function fromProtoRibltCodewords(msg: any): RibltCodewords {
  return {
    filterId: msg.filterId,
    round: msg.round,
    startIndex: msg.startIndex,
    codewords: (msg.codewords ?? []).map((cw: any) => ({
      count: cw.count,
      keySum: cw.keySum,
      valueSum: cw.valueSum,
    })),
  };
}

function toProtoRibltStatus(status: RibltStatus) {
  if (status.payload.case === "decoded") {
    return create(RibltStatusSchema, {
      filterId: status.filterId,
      round: status.round,
      payload: {
        case: "decoded",
        value: create(RibltDecodedSchema, {
          senderMissing: status.payload.value.senderMissing.map(toProtoOpRef),
          receiverMissing: status.payload.value.receiverMissing.map(toProtoOpRef),
          codewordsReceived: status.payload.value.codewordsReceived,
        }),
      },
    });
  }

  return create(RibltStatusSchema, {
    filterId: status.filterId,
    round: status.round,
    payload: {
      case: "failed",
      value: create(RibltFailedSchema, {
        reason: status.payload.value.reason,
        message: status.payload.value.message ?? "",
      }),
    },
  });
}

function fromProtoRibltStatus(status: any): RibltStatus {
  if (status.payload.case === "decoded") {
    const decoded = status.payload.value;
    return {
      filterId: status.filterId,
      round: status.round,
      payload: {
        case: "decoded",
        value: {
          senderMissing: decoded.senderMissing.map(fromProtoOpRef),
          receiverMissing: decoded.receiverMissing.map(fromProtoOpRef),
          codewordsReceived: decoded.codewordsReceived,
        },
      },
    };
  }
  if (status.payload.case === "failed") {
    const failed = status.payload.value;
    return {
      filterId: status.filterId,
      round: status.round,
      payload: {
        case: "failed",
        value: {
          reason: failed.reason,
          message: failed.message?.length > 0 ? failed.message : undefined,
        },
      },
    };
  }
  return {
    filterId: status.filterId,
    round: status.round,
    payload: { case: "failed", value: { reason: RibltFailureReason.DECODE_FAILED } },
  };
}

export function encodeTreecrdtSyncV0(msg: SyncMessage<Operation>): Uint8Array {
  if (msg.v !== 0) {
    throw new Error(`encodeTreecrdtSyncV0: unsupported version: ${msg.v}`);
  }

  const base = { v: 0, docId: msg.docId };

  switch (msg.payload.case) {
    case "hello": {
      const proto = create(SyncMessageSchema, { ...base, payload: { case: "hello", value: toProtoHello(msg.payload.value) } });
      return toBinary(SyncMessageSchema, proto);
    }
    case "helloAck": {
      const proto = create(SyncMessageSchema, {
        ...base,
        payload: { case: "helloAck", value: toProtoHelloAck(msg.payload.value) },
      });
      return toBinary(SyncMessageSchema, proto);
    }
    case "ribltCodewords": {
      const proto = create(SyncMessageSchema, {
        ...base,
        payload: { case: "ribltCodewords", value: toProtoRibltCodewords(msg.payload.value) },
      });
      return toBinary(SyncMessageSchema, proto);
    }
    case "ribltStatus": {
      const proto = create(SyncMessageSchema, {
        ...base,
        payload: { case: "ribltStatus", value: toProtoRibltStatus(msg.payload.value) },
      });
      return toBinary(SyncMessageSchema, proto);
    }
    case "opsBatch": {
      const proto = create(SyncMessageSchema, { ...base, payload: { case: "opsBatch", value: toProtoOpsBatch(msg.payload.value) } });
      return toBinary(SyncMessageSchema, proto);
    }
    case "subscribe": {
      const proto = create(SyncMessageSchema, { ...base, payload: { case: "subscribe", value: toProtoSubscribe(msg.payload.value) } });
      return toBinary(SyncMessageSchema, proto);
    }
    case "subscribeAck": {
      const proto = create(SyncMessageSchema, {
        ...base,
        payload: { case: "subscribeAck", value: toProtoSubscribeAck(msg.payload.value) },
      });
      return toBinary(SyncMessageSchema, proto);
    }
    case "unsubscribe": {
      const proto = create(SyncMessageSchema, { ...base, payload: { case: "unsubscribe", value: toProtoUnsubscribe(msg.payload.value) } });
      return toBinary(SyncMessageSchema, proto);
    }
    case "error": {
      const err = msg.payload.value;
      const protoErr = create(SyncErrorSchema, {
        code: err.code,
        message: err.message,
        filterId: err.filterId ?? "",
        subscriptionId: err.subscriptionId ?? "",
      });
      const proto = create(SyncMessageSchema, { ...base, payload: { case: "error", value: protoErr } });
      return toBinary(SyncMessageSchema, proto);
    }
    default: {
      const _exhaustive: never = msg.payload;
      throw new Error(`encodeTreecrdtSyncV0: unsupported payload: ${String((_exhaustive as any)?.case)}`);
    }
  }
}

export function decodeTreecrdtSyncV0(bytes: Uint8Array): SyncMessage<Operation> {
  const msg = fromBinary(SyncMessageSchema, bytes);
  if (msg.v !== 0) {
    throw new Error(`decodeTreecrdtSyncV0: unsupported version: ${msg.v}`);
  }
  const base = { v: 0 as const, docId: msg.docId } as const;

  switch (msg.payload.case) {
    case "hello":
      return { ...base, payload: { case: "hello", value: fromProtoHello(msg.payload.value) } };
    case "helloAck":
      return { ...base, payload: { case: "helloAck", value: fromProtoHelloAck(msg.payload.value) } };
    case "ribltCodewords":
      return { ...base, payload: { case: "ribltCodewords", value: fromProtoRibltCodewords(msg.payload.value) } };
    case "ribltStatus":
      return { ...base, payload: { case: "ribltStatus", value: fromProtoRibltStatus(msg.payload.value) } };
    case "opsBatch":
      return { ...base, payload: { case: "opsBatch", value: fromProtoOpsBatch(msg.payload.value) } };
    case "subscribe":
      return { ...base, payload: { case: "subscribe", value: fromProtoSubscribe(msg.payload.value) } };
    case "subscribeAck":
      return { ...base, payload: { case: "subscribeAck", value: fromProtoSubscribeAck(msg.payload.value) } };
    case "unsubscribe":
      return { ...base, payload: { case: "unsubscribe", value: fromProtoUnsubscribe(msg.payload.value) } };
    case "error": {
      const err = msg.payload.value;
      return {
        ...base,
        payload: {
          case: "error",
          value: {
            code: err.code,
            message: err.message,
            filterId: err.filterId.length > 0 ? err.filterId : undefined,
            subscriptionId: err.subscriptionId.length > 0 ? err.subscriptionId : undefined,
          },
        },
      };
    }
    default:
      throw new Error(`decodeTreecrdtSyncV0: unsupported payload: ${String(msg.payload.case)}`);
  }
}

export const treecrdtSyncV0ProtobufCodec: WireCodec<SyncMessage<Operation>, Uint8Array> = {
  encode: encodeTreecrdtSyncV0,
  decode: decodeTreecrdtSyncV0,
};

export function bytesToNodeId(bytes: Bytes): string {
  return nodeIdFromBytes16(bytes);
}

export function nodeIdToBytes16(nodeId: string): Uint8Array {
  return nodeIdToBytes16Impl(nodeId);
}
