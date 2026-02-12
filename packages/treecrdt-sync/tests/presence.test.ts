import { expect, test } from "vitest";

import { createBroadcastPresenceMesh } from "../dist/presence.js";
import type { BroadcastChannelLike, WireCodec } from "../dist/transport.js";

type MessageListener = (event: { data: unknown }) => void;

class MockBroadcastBus {
  private readonly channelsByName = new Map<string, Set<MockBroadcastChannel>>();
  dropFirstAckAToB = true;

  createChannel(name: string, instanceId: string): MockBroadcastChannel {
    const ch = new MockBroadcastChannel(this, name, instanceId);
    let set = this.channelsByName.get(name);
    if (!set) {
      set = new Set();
      this.channelsByName.set(name, set);
    }
    set.add(ch);
    return ch;
  }

  removeChannel(channel: MockBroadcastChannel) {
    const set = this.channelsByName.get(channel.name);
    if (!set) return;
    set.delete(channel);
    if (set.size === 0) this.channelsByName.delete(channel.name);
  }

  post(sender: MockBroadcastChannel, message: unknown) {
    const set = this.channelsByName.get(sender.name);
    if (!set) return;
    for (const target of set) {
      if (this.shouldDropMessage(message, target)) continue;
      queueMicrotask(() => target._deliver(message));
    }
  }

  private shouldDropMessage(message: unknown, target: MockBroadcastChannel): boolean {
    if (!this.dropFirstAckAToB) return false;
    if (!message || typeof message !== "object") return false;
    const t = (message as any).t;
    if (t !== "presence_ack") return false;
    const peerId = (message as any).peer_id;
    const toPeerId = (message as any).to_peer_id;
    if (peerId !== "a" || toPeerId !== "b") return false;
    if (target.instanceId !== "b") return false;
    this.dropFirstAckAToB = false;
    return true;
  }
}

class MockBroadcastChannel implements BroadcastChannelLike {
  readonly name: string;
  readonly instanceId: string;
  private closed = false;
  private readonly listeners = new Set<MessageListener>();

  constructor(
    private readonly bus: MockBroadcastBus,
    name: string,
    instanceId: string
  ) {
    this.name = name;
    this.instanceId = instanceId;
  }

  postMessage(message: unknown): void {
    if (this.closed) throw new Error("Channel is closed");
    this.bus.post(this, message);
  }

  addEventListener(type: "message", listener: MessageListener): void {
    if (this.closed) throw new Error("Channel is closed");
    if (type !== "message") return;
    this.listeners.add(listener);
  }

  removeEventListener(type: "message", listener: MessageListener): void {
    if (type !== "message") return;
    this.listeners.delete(listener);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.listeners.clear();
    this.bus.removeChannel(this);
  }

  _deliver(data: unknown) {
    if (this.closed) return;
    for (const listener of this.listeners) listener({ data });
  }
}

async function waitUntil(
  predicate: () => Promise<boolean> | boolean,
  opts: { timeoutMs?: number; intervalMs?: number } = {}
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 250;
  const intervalMs = opts.intervalMs ?? 5;
  const start = Date.now();
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const ok = await predicate();
    if (ok) return;
    if (Date.now() - start > timeoutMs) throw new Error(`waitUntil timeout after ${timeoutMs}ms`);
    await new Promise<void>((resolve) => setTimeout(resolve, intervalMs));
  }
}

const noopCodec: WireCodec<unknown, Uint8Array> = {
  encode: () => new Uint8Array(),
  decode: () => ({}),
};

test("presence mesh retries ack delivery so peers become ready after missed ack", async () => {
  const bus = new MockBroadcastBus();

  let readyA = false;
  let readyB = false;

  const channelA = bus.createChannel("presence", "a");
  const meshA = createBroadcastPresenceMesh({
    channel: channelA,
    selfId: "a",
    codec: noopCodec,
    presenceIntervalMs: 10,
    pruneIntervalMs: 50,
    peerTimeoutMs: 200,
    createChannel: (name) => bus.createChannel(name, "a"),
    onPeerReady: (peerId) => {
      if (peerId === "b") readyA = true;
    },
  });

  const channelB = bus.createChannel("presence", "b");
  const meshB = createBroadcastPresenceMesh({
    channel: channelB,
    selfId: "b",
    codec: noopCodec,
    presenceIntervalMs: 10,
    pruneIntervalMs: 50,
    peerTimeoutMs: 200,
    createChannel: (name) => bus.createChannel(name, "b"),
    onPeerReady: (peerId) => {
      if (peerId === "a") readyB = true;
    },
  });

  try {
    await waitUntil(() => readyA && readyB, { timeoutMs: 500 });
    expect(meshA.isPeerReady("b")).toBe(true);
    expect(meshB.isPeerReady("a")).toBe(true);
  } finally {
    meshA.stop();
    meshB.stop();
    channelA.close();
    channelB.close();
  }
});

