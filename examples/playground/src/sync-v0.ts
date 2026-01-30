import type { Operation } from "@treecrdt/interface";
import { bytesToHex as bytesToHexImpl, nodeIdToBytes16 } from "@treecrdt/interface/ids";
import type { TreecrdtClient } from "@treecrdt/wa-sqlite/client";
import type { DuplexTransport, WireCodec } from "@treecrdt/sync/transport";
import { createTreecrdtSyncSqlitePendingOpsStore, type Filter, type OpRef, type SyncBackend, type SyncMessage } from "@treecrdt/sync";

export type PresenceMessage = {
  t: "presence";
  peer_id: string;
  ts: number;
};

export type PresenceAckMessage = {
  t: "presence_ack";
  peer_id: string;
  to_peer_id: string;
  ts: number;
};

export type PlaygroundBroadcastMessage = PresenceMessage | PresenceAckMessage;

export function hexToBytes16(hex: string): Uint8Array {
  return nodeIdToBytes16(hex);
}

export function bytesToHex(bytes: Uint8Array): string {
  return bytesToHexImpl(bytes);
}

export function createPlaygroundBackend(
  client: TreecrdtClient,
  docId: string,
  opts: { enablePendingSidecar?: boolean } = {}
): SyncBackend<Operation> {
  const pending = opts.enablePendingSidecar
    ? createTreecrdtSyncSqlitePendingOpsStore({ runner: client.runner, docId })
    : null;
  let pendingReady = false;
  const ensurePendingReady = async () => {
    if (!pending || pendingReady) return;
    await pending.init();
    pendingReady = true;
  };

  return {
    docId,

    async maxLamport() {
      const ops = await client.ops.all();
      const max = ops.reduce((acc, op) => Math.max(acc, op.meta.lamport), 0);
      return BigInt(max);
    },

    async listOpRefs(filter: Filter) {
      if ("all" in filter) {
        const refs = await client.opRefs.all();
        if (!pending) return refs;
        await ensurePendingReady();
        const pendingRefs = await pending.listPendingOpRefs();
        if (pendingRefs.length === 0) return refs;

        // Union with pending refs so reconcile doesn't re-download quarantined ops.
        const byHex = new Map(refs.map((r) => [bytesToHexImpl(r), r]));
        for (const r of pendingRefs) byHex.set(bytesToHexImpl(r), r);
        return Array.from(byHex.values());
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

    ...(pending
      ? {
          storePendingOps: async (ops) => {
            await ensurePendingReady();
            await pending.storePendingOps(ops);
          },
          listPendingOps: async () => {
            await ensurePendingReady();
            return pending.listPendingOps();
          },
          deletePendingOps: async (ops) => {
            await ensurePendingReady();
            await pending.deletePendingOps(ops);
          },
        }
      : {}),
  };
}

export function createBroadcastDuplex<Op>(
  channel: BroadcastChannel,
  selfId: string,
  peerId: string,
  codec: WireCodec<SyncMessage<Op>, Uint8Array>
): DuplexTransport<SyncMessage<Op>> & { close: () => void } {
  const incoming = new BroadcastChannel(`${channel.name}:sync:${peerId}->${selfId}`);
  const outgoing = new BroadcastChannel(`${channel.name}:sync:${selfId}->${peerId}`);
  const handlers = new Set<(msg: SyncMessage<Op>) => void>();
  let listening = false;
  const debug =
    typeof window !== "undefined" && new URLSearchParams(window.location.search).has("debugSync");

  const onMessage = (ev: MessageEvent<any>) => {
    const data = ev.data as unknown;
    let bytes: Uint8Array | null = null;
    if (data instanceof Uint8Array) bytes = data;
    else if (data instanceof ArrayBuffer) bytes = new Uint8Array(data);
    else if (ArrayBuffer.isView(data) && data.buffer instanceof ArrayBuffer) {
      bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }
    if (!bytes) return;

    let msg: SyncMessage<Op>;
    try {
      msg = codec.decode(bytes);
    } catch (err) {
      if (debug) console.debug(`[sync:${selfId}] decode error from ${peerId}: ${String(err)}`);
      return;
    }
    if (debug) {
      const detail =
        msg.payload.case === "error"
          ? ` code=${msg.payload.value.code} message=${msg.payload.value.message}`
          : msg.payload.case === "hello"
            ? ` caps=${msg.payload.value.capabilities.length} filters=${msg.payload.value.filters.length}`
            : msg.payload.case === "helloAck"
              ? ` caps=${msg.payload.value.capabilities.length} accepted=${msg.payload.value.acceptedFilters.length} rejected=${msg.payload.value.rejectedFilters.length}`
              : "";
      console.debug(`[sync:${selfId}] recv ${msg.payload.case} from ${peerId}${detail}`);
    }
    for (const h of handlers) h(msg);
  };

  return {
    async send(msg) {
      if (debug) {
        const detail =
          msg.payload.case === "error"
            ? ` code=${msg.payload.value.code} message=${msg.payload.value.message}`
            : msg.payload.case === "hello"
              ? ` caps=${msg.payload.value.capabilities.length} filters=${msg.payload.value.filters.length}`
              : msg.payload.case === "helloAck"
                ? ` caps=${msg.payload.value.capabilities.length} accepted=${msg.payload.value.acceptedFilters.length} rejected=${msg.payload.value.rejectedFilters.length}`
                : "";
        console.debug(`[sync:${selfId}] send ${msg.payload.case} to ${peerId}${detail}`);
      }
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
