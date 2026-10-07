import type { Operation } from '@treecrdt/interface';
import { normalizeNodeId } from '@treecrdt/interface/ids';

// Serde's byte-buffer reader uses realm-local instanceof checks.
function localByteView(bytes: Uint8Array): Uint8Array {
  if (
    bytes instanceof Uint8Array ||
    !ArrayBuffer.isView(bytes) ||
    Object.prototype.toString.call(bytes) !== '[object Uint8Array]'
  ) {
    return bytes;
  }
  const view = bytes as Uint8Array;
  return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
}

export function toWasmOperation(
  op: Operation,
  serializeReplica?: (replica: Operation['meta']['id']['replica']) => Uint8Array,
): Operation {
  if (op.kind.type === 'payload' && op.kind.payload === undefined) {
    throw new Error('treecrdt: payload operations require payload or null');
  }
  const kind = { ...op.kind, node: normalizeNodeId(op.kind.node) };
  if (kind.type === 'insert') kind.parent = normalizeNodeId(kind.parent);
  else if (kind.type === 'move') kind.newParent = normalizeNodeId(kind.newParent);
  if ('orderKey' in kind) kind.orderKey = localByteView(kind.orderKey);
  if ('payload' in kind && kind.payload) kind.payload = localByteView(kind.payload);
  return {
    meta: {
      ...op.meta,
      id: {
        ...op.meta.id,
        replica: serializeReplica
          ? new Uint8Array(serializeReplica(op.meta.id.replica))
          : localByteView(op.meta.id.replica),
      },
      knownState: op.meta.knownState && localByteView(op.meta.knownState),
    },
    kind,
  };
}
