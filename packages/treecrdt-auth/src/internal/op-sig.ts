import { utf8ToBytes } from '@noble/hashes/utils';

import type { Operation } from '@treecrdt/interface';
import { nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';

import { signEd25519, verifyEd25519 } from '../ed25519.js';
import { concatBytes, u32be, u64be, u8 } from './bytes.js';

const OP_SIG_V1_DOMAIN = utf8ToBytes('treecrdt/op-sig/v1');
const OP_SIG_V2_DOMAIN = utf8ToBytes('treecrdt/op-sig/v2');

export type TreecrdtOpAuthClaimsV1 = {
  authoredAtMs?: number;
};

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

function encodeTreecrdtOpAuthClaimsV1(claims: TreecrdtOpAuthClaimsV1): Uint8Array {
  const parts: Uint8Array[] = [];
  if (claims.authoredAtMs === undefined) {
    parts.push(u8(0));
  } else {
    if (!Number.isSafeInteger(claims.authoredAtMs) || claims.authoredAtMs < 0) {
      throw new Error(`authoredAtMs must be a non-negative safe integer: ${claims.authoredAtMs}`);
    }
    parts.push(u8(1), u64be(claims.authoredAtMs));
  }
  return concatBytes(...parts);
}

function encodeTreecrdtOpKnownStateV2(op: Operation): Uint8Array {
  const knownState = op.meta.knownState;
  if (knownState === undefined) return u8(0);
  return concatBytes(u8(1), u32be(knownState.length), knownState);
}

export function encodeTreecrdtOpSigInputV1(opts: { docId: string; op: Operation }): Uint8Array {
  return concatBytes(OP_SIG_V1_DOMAIN, u8(0), encodeTreecrdtOpFields(opts));
}

export function encodeTreecrdtOpSigInputV2(opts: {
  docId: string;
  op: Operation;
  claims: TreecrdtOpAuthClaimsV1;
}): Uint8Array {
  return concatBytes(
    OP_SIG_V2_DOMAIN,
    u8(0),
    encodeTreecrdtOpFields(opts),
    encodeTreecrdtOpKnownStateV2(opts.op),
    encodeTreecrdtOpAuthClaimsV1(opts.claims),
  );
}

export async function signTreecrdtOpV1(opts: {
  docId: string;
  op: Operation;
  privateKey: Uint8Array;
}): Promise<Uint8Array> {
  const msg = encodeTreecrdtOpSigInputV1({ docId: opts.docId, op: opts.op });
  return signEd25519(msg, opts.privateKey);
}

export async function signTreecrdtOpV2(opts: {
  docId: string;
  op: Operation;
  privateKey: Uint8Array;
  claims: TreecrdtOpAuthClaimsV1;
}): Promise<Uint8Array> {
  const msg = encodeTreecrdtOpSigInputV2({
    docId: opts.docId,
    op: opts.op,
    claims: opts.claims,
  });
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

export async function verifyTreecrdtOpV2(opts: {
  docId: string;
  op: Operation;
  signature: Uint8Array;
  publicKey: Uint8Array;
  claims: TreecrdtOpAuthClaimsV1;
}): Promise<boolean> {
  const msg = encodeTreecrdtOpSigInputV2({
    docId: opts.docId,
    op: opts.op,
    claims: opts.claims,
  });
  return await verifyEd25519(opts.signature, msg, opts.publicKey);
}
