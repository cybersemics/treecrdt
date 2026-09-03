import { utf8ToBytes } from '@noble/hashes/utils';

import type { Operation } from '@treecrdt/interface';
import { nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';
import { decodeVersionVectorV0 } from '@treecrdt/interface/version-vector';

import { signEd25519, verifyEd25519 } from '../ed25519.js';
import { concatBytes, u32be, u64be, u8 } from './bytes.js';

const OP_SIG_DOMAIN = utf8ToBytes('treecrdt/op-sig/v1');
const MAX_KNOWN_STATE_BYTES = 1024 * 1024;
const MAX_KNOWN_STATE_ENTRIES = 4096;
const V0_REPLICA_ID_BYTES = 32;

function encodeTreecrdtOpFields(opts: { docId: string; op: Operation }): Uint8Array {
  const docIdBytes = utf8ToBytes(opts.docId);
  const replicaBytes = replicaIdToBytes(opts.op.meta.id.replica);

  const counter = opts.op.meta.id.counter;
  const lamport = opts.op.meta.lamport;

  let kindTag: number;
  let kindFields: Uint8Array;

  switch (opts.op.kind.type) {
    case 'insert': {
      kindTag = 1;
      const parent = nodeIdToBytes16(opts.op.kind.parent);
      const node = nodeIdToBytes16(opts.op.kind.node);
      const orderKey = opts.op.kind.orderKey;
      const orderKeyLen = u32be(orderKey.length);
      const payload = opts.op.kind.payload;
      if (payload) {
        kindFields = concatBytes(
          parent,
          node,
          orderKeyLen,
          orderKey,
          u8(1),
          u32be(payload.length),
          payload,
        );
      } else {
        kindFields = concatBytes(parent, node, orderKeyLen, orderKey, u8(0));
      }
      break;
    }
    case 'move': {
      kindTag = 2;
      const node = nodeIdToBytes16(opts.op.kind.node);
      const newParent = nodeIdToBytes16(opts.op.kind.newParent);
      const orderKey = opts.op.kind.orderKey;
      const orderKeyLen = u32be(orderKey.length);
      kindFields = concatBytes(node, newParent, orderKeyLen, orderKey);
      break;
    }
    case 'delete': {
      kindTag = 3;
      const node = nodeIdToBytes16(opts.op.kind.node);
      kindFields = node;
      break;
    }
    case 'tombstone': {
      kindTag = 4;
      const node = nodeIdToBytes16(opts.op.kind.node);
      kindFields = node;
      break;
    }
    case 'payload': {
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
    u32be(docIdBytes.length),
    docIdBytes,
    u32be(replicaBytes.length),
    replicaBytes,
    u64be(counter),
    u64be(lamport),
    u8(kindTag),
    kindFields,
  );
}

function encodeKnownState(knownState: Uint8Array | undefined): Uint8Array {
  return knownState === undefined
    ? u8(0)
    : concatBytes(u8(1), u32be(knownState.length), knownState);
}

function invalidKnownState(): never {
  throw new Error(
    'knownState must use canonical TreeCRDT VersionVector v0 bytes with 32-byte replica ids',
  );
}

function assertKnownStateSize(bytes: Uint8Array): void {
  if (bytes.length > MAX_KNOWN_STATE_BYTES) {
    throw new Error(
      `knownState exceeds the ${MAX_KNOWN_STATE_BYTES}-byte operation-signature limit`,
    );
  }
}

function assertCanonicalKnownState(bytes: Uint8Array): void {
  assertKnownStateSize(bytes);
  let vector: ReturnType<typeof decodeVersionVectorV0>;
  try {
    vector = decodeVersionVectorV0(bytes);
  } catch {
    return invalidKnownState();
  }
  if (vector.entries.length > MAX_KNOWN_STATE_ENTRIES) {
    throw new Error(
      `knownState exceeds the ${MAX_KNOWN_STATE_ENTRIES}-entry operation-signature limit`,
    );
  }
  for (const entry of vector.entries) {
    if (entry.replica.length !== V0_REPLICA_ID_BYTES) invalidKnownState();
  }
}

function assertPolicyOperation(op: Operation, knownState: Uint8Array | undefined): void {
  if (knownState !== undefined) assertKnownStateSize(knownState);
  if (op.kind.type === 'delete') {
    if (knownState === undefined || knownState.length === 0) {
      throw new Error('delete operations require non-empty knownState');
    }
  } else if (knownState !== undefined) {
    throw new Error('knownState is only allowed on delete operations');
  }
}

function assertCanonicalOperationKnownState(knownState: Uint8Array | undefined): void {
  if (knownState !== undefined) assertCanonicalKnownState(knownState);
}

function encodeTreecrdtOpSigInputUnchecked(
  opts: { docId: string; op: Operation },
  knownState: Uint8Array | undefined,
): { message: Uint8Array; signedKnownState: Uint8Array | undefined } {
  const message = concatBytes(
    OP_SIG_DOMAIN,
    u8(0),
    encodeTreecrdtOpFields(opts),
    encodeKnownState(knownState),
  );
  return {
    message,
    signedKnownState:
      knownState === undefined ? undefined : message.subarray(message.length - knownState.length),
  };
}

export function encodeTreecrdtOpSigInput(opts: { docId: string; op: Operation }): Uint8Array {
  const knownState = opts.op.meta.knownState;
  assertPolicyOperation(opts.op, knownState);
  const encoded = encodeTreecrdtOpSigInputUnchecked(opts, knownState);
  assertCanonicalOperationKnownState(encoded.signedKnownState);
  return encoded.message;
}

export async function signTreecrdtOp(opts: {
  docId: string;
  op: Operation;
  privateKey: Uint8Array;
}): Promise<Uint8Array> {
  const msg = encodeTreecrdtOpSigInput({ docId: opts.docId, op: opts.op });
  return signEd25519(msg, opts.privateKey);
}

export async function verifyTreecrdtOp(opts: {
  docId: string;
  op: Operation;
  signature: Uint8Array;
  publicKey: Uint8Array;
}): Promise<boolean> {
  const knownState = opts.op.meta.knownState;
  assertPolicyOperation(opts.op, knownState);
  const encoded = encodeTreecrdtOpSigInputUnchecked({ docId: opts.docId, op: opts.op }, knownState);
  const verified = await verifyEd25519(opts.signature, encoded.message, opts.publicKey);
  if (!verified) return false;
  // Validate the suffix copied into the signed message, not the caller-owned mutable input.
  assertCanonicalOperationKnownState(encoded.signedKnownState);
  return true;
}
