import { utf8ToBytes } from "@noble/hashes/utils";

import type { Operation } from "@treecrdt/interface";
import { nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";

import { signEd25519, verifyEd25519 } from "../ed25519.js";
import { concatBytes, u32be, u64be, u8 } from "./bytes.js";

const OP_SIG_V1_DOMAIN = utf8ToBytes("treecrdt/op-sig/v1");

export function encodeTreecrdtOpSigInputV1(opts: { docId: string; op: Operation }): Uint8Array {
  const docIdBytes = utf8ToBytes(opts.docId);
  const replicaBytes = replicaIdToBytes(opts.op.meta.id.replica);

  const counter = opts.op.meta.id.counter;
  const lamport = opts.op.meta.lamport;

  let kindTag: number;
  let kindFields: Uint8Array;

  switch (opts.op.kind.type) {
    case "insert": {
      kindTag = 1;
      const parent = nodeIdToBytes16(opts.op.kind.parent);
      const node = nodeIdToBytes16(opts.op.kind.node);
      const orderKey = opts.op.kind.orderKey;
      const orderKeyLen = u32be(orderKey.length);
      const payload = opts.op.kind.payload;
      if (payload) {
        kindFields = concatBytes(parent, node, orderKeyLen, orderKey, u8(1), u32be(payload.length), payload);
      } else {
        kindFields = concatBytes(parent, node, orderKeyLen, orderKey, u8(0));
      }
      break;
    }
    case "move": {
      kindTag = 2;
      const node = nodeIdToBytes16(opts.op.kind.node);
      const newParent = nodeIdToBytes16(opts.op.kind.newParent);
      const orderKey = opts.op.kind.orderKey;
      const orderKeyLen = u32be(orderKey.length);
      kindFields = concatBytes(node, newParent, orderKeyLen, orderKey);
      break;
    }
    case "delete": {
      kindTag = 3;
      const node = nodeIdToBytes16(opts.op.kind.node);
      kindFields = node;
      break;
    }
    case "tombstone": {
      kindTag = 4;
      const node = nodeIdToBytes16(opts.op.kind.node);
      kindFields = node;
      break;
    }
    case "payload": {
      kindTag = 5;
      const node = nodeIdToBytes16(opts.op.kind.node);
      const payload = opts.op.kind.payload;
      if (payload === null) {
        kindFields = concatBytes(node, u8(0));
      } else {
        kindFields = concatBytes(node, u8(1), u32be(payload.length), payload);
      }
      break;
    }
    default: {
      const _exhaustive: never = opts.op.kind;
      throw new Error(`unknown op kind: ${String((_exhaustive as any)?.type)}`);
    }
  }

  return concatBytes(
    OP_SIG_V1_DOMAIN,
    u8(0),
    u32be(docIdBytes.length),
    docIdBytes,
    u32be(replicaBytes.length),
    replicaBytes,
    u64be(counter),
    u64be(lamport),
    u8(kindTag),
    kindFields
  );
}

export async function signTreecrdtOpV1(opts: { docId: string; op: Operation; privateKey: Uint8Array }): Promise<Uint8Array> {
  const msg = encodeTreecrdtOpSigInputV1({ docId: opts.docId, op: opts.op });
  return signEd25519(msg, opts.privateKey);
}

export async function verifyTreecrdtOpV1(opts: {
  docId: string;
  op: Operation;
  signature: Uint8Array;
  publicKey: Uint8Array;
}): Promise<boolean> {
  const msg = encodeTreecrdtOpSigInputV1({ docId: opts.docId, op: opts.op });
  return await verifyEd25519(opts.signature, msg, opts.publicKey);
}

