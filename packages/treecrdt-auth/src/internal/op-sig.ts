import { utf8ToBytes } from '@noble/hashes/utils';

import type { Operation } from '@treecrdt/interface';
import { nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';

import { signEd25519, verifyEd25519 } from '../ed25519.js';
import { concatBytes, u32be, u64be, u8 } from './bytes.js';

const OP_SIG_DOMAIN = utf8ToBytes('treecrdt/op-sig/v1');

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

function encodeKnownState(op: Operation): Uint8Array {
  const knownState = op.meta.knownState;
  return knownState === undefined || knownState.length === 0
    ? u8(0)
    : concatBytes(u8(1), u32be(knownState.length), assertCanonicalKnownState(knownState));
}

function invalidKnownState(): never {
  throw new Error(
    'knownState must use canonical TreeCRDT v0 version-vector JSON with counters within Number.MAX_SAFE_INTEGER',
  );
}

function isV0VersionVectorCounter(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function assertCanonicalKnownState(bytes: Uint8Array): Uint8Array {
  let parsed: any;
  try {
    parsed = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  } catch {
    return invalidKnownState();
  }
  if (!parsed || typeof parsed !== 'object' || !Array.isArray(parsed.entries)) {
    return invalidKnownState();
  }

  const replicas = new Set<string>();
  const entries = parsed.entries.map((entry: any) => {
    if (
      !entry ||
      typeof entry !== 'object' ||
      !Array.isArray(entry.replica) ||
      !entry.replica.every(
        (byte: unknown) =>
          typeof byte === 'number' && Number.isInteger(byte) && byte >= 0 && byte <= 255,
      ) ||
      !isV0VersionVectorCounter(entry.frontier) ||
      !Array.isArray(entry.ranges)
    ) {
      return invalidKnownState();
    }

    // Rust keeps ranges normalized: positive inclusive bounds, separated by at least one
    // missing counter, and strictly beyond the contiguous frontier.
    let previousEnd = entry.frontier;
    for (const range of entry.ranges) {
      if (
        !Array.isArray(range) ||
        range.length !== 2 ||
        !isV0VersionVectorCounter(range[0]) ||
        !isV0VersionVectorCounter(range[1]) ||
        range[0] === 0 ||
        range[0] > range[1] ||
        range[0] - previousEnd <= 1
      ) {
        return invalidKnownState();
      }
      previousEnd = range[1];
    }

    const replicaKey = entry.replica.join(',');
    if (replicas.has(replicaKey)) return invalidKnownState();
    replicas.add(replicaKey);
    return {
      replica: entry.replica,
      frontier: entry.frontier,
      ranges: entry.ranges,
    };
  });
  entries.sort((a: any, b: any) => {
    const length = Math.min(a.replica.length, b.replica.length);
    for (let i = 0; i < length; i += 1) {
      if (a.replica[i] !== b.replica[i]) return a.replica[i] - b.replica[i];
    }
    return a.replica.length - b.replica.length;
  });

  const canonical = utf8ToBytes(JSON.stringify({ entries }));
  if (canonical.length !== bytes.length || canonical.some((byte, index) => byte !== bytes[index])) {
    return invalidKnownState();
  }
  return bytes;
}

function assertPolicyOperation(op: Operation): void {
  const hasKnownState = op.meta.knownState !== undefined && op.meta.knownState.length > 0;
  if (op.kind.type === 'delete' && !hasKnownState) {
    throw new Error('delete operations require non-empty knownState');
  }
  if (op.kind.type !== 'delete' && hasKnownState) {
    throw new Error('knownState is only allowed on delete operations');
  }
}

export function encodeTreecrdtOpSigInput(opts: { docId: string; op: Operation }): Uint8Array {
  assertPolicyOperation(opts.op);
  return concatBytes(OP_SIG_DOMAIN, u8(0), encodeTreecrdtOpFields(opts), encodeKnownState(opts.op));
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
  const msg = encodeTreecrdtOpSigInput({ docId: opts.docId, op: opts.op });
  return await verifyEd25519(opts.signature, msg, opts.publicKey);
}
