import type { Operation } from "@treecrdt/interface";
import { bytesToHex as bytesToHexImpl, nodeIdToBytes16 } from "@treecrdt/interface/ids";
import type { TreecrdtClient } from "@treecrdt/wa-sqlite/client";
import type { DuplexTransport, WireCodec } from "@treecrdt/sync/transport";
import type { Filter, OpRef, SyncBackend, SyncMessage } from "@treecrdt/sync";

export type PresenceMessage = {
  t: "presence";
  peer_id: string;
  ts: number;
};

export function hexToBytes16(hex: string): Uint8Array {
  return nodeIdToBytes16(hex);
}

export function bytesToHex(bytes: Uint8Array): string {
  return bytesToHexImpl(bytes);
}

export function createPlaygroundBackend(client: TreecrdtClient, docId: string): SyncBackend<Operation> {
  return {
    docId,

    async maxLamport() {
      const ops = await client.ops.all();
      const max = ops.reduce((acc, op) => Math.max(acc, op.meta.lamport), 0);
      return BigInt(max);
    },

    async listOpRefs(filter: Filter) {
      if ("all" in filter) {
        return client.opRefs.all();
      }
      return client.opRefs.children(bytesToHex(filter.children.parent));
    },

    async getOpsByOpRefs(opRefs: OpRef[]) {
      return client.ops.get(opRefs);
    },

    async applyOps(ops: Operation[]) {
      if (ops.length === 0) return;
      await client.ops.appendMany(ops);
    },
  };
}

export function createBroadcastDuplex<Op>(
  channel: BroadcastChannel,
  selfId: string,
  peerId: string,
  codec: WireCodec<SyncMessage<Op>, Uint8Array>
): DuplexTransport<SyncMessage<Op>> {
  const incoming = new BroadcastChannel(`${channel.name}:sync:${peerId}->${selfId}`);
  const outgoing = new BroadcastChannel(`${channel.name}:sync:${selfId}->${peerId}`);
  const handlers = new Set<(msg: SyncMessage<Op>) => void>();
  let listening = false;

  const onMessage = (ev: MessageEvent<any>) => {
    const bytes = ev.data as unknown;
    if (!(bytes instanceof Uint8Array)) return;
    const msg = codec.decode(bytes);
    for (const h of handlers) h(msg);
  };

  return {
    async send(msg) {
      outgoing.postMessage(codec.encode(msg));
    },
    onMessage(handler) {
      handlers.add(handler);
      if (!listening) {
        incoming.addEventListener("message", onMessage as any);
        listening = true;
      }

      return () => {
        handlers.delete(handler);
        if (handlers.size === 0 && listening) {
          incoming.removeEventListener("message", onMessage as any);
          listening = false;
        }
      };
    },
    close() {
      incoming.close();
      outgoing.close();
    },
  };
}
