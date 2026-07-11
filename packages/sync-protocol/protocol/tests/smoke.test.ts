import { createHash } from 'node:crypto';

import { expect, test } from 'vitest';

import type { Operation } from '@treecrdt/interface';
import { bytesToHex, nodeIdToBytes16 } from '@treecrdt/interface/ids';
import { makeOp, nodeIdFromInt } from '@treecrdt/benchmark';

import { treecrdtSyncV0ProtobufCodec } from '../dist/protobuf.js';
import { SyncPeer } from '../dist/sync.js';
import { createInMemoryConnectedPeers } from '../dist/in-memory.js';
import { wrapDuplexTransportWithCodec } from '../dist/transport/index.js';
import type { DuplexTransport } from '../dist/transport/index.js';
import { ErrorCode } from '../dist/types.js';
import type { Filter, OpRef, SyncBackend, SyncMessage } from '../dist/types.js';

function opRefFor(docId: string, replica: string, counter: number): OpRef {
  const h = createHash('sha256');
  h.update('treecrdt/opref/test');
  h.update(docId);
  h.update(replica);
  h.update(String(counter));
  return new Uint8Array(h.digest()).slice(0, 16);
}

function setHex(opRefs: readonly Uint8Array[]): Set<string> {
  return new Set(opRefs.map((r) => bytesToHex(r)));
}

async function tick(): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
}

async function sendHello(
  transport: DuplexTransport<SyncMessage<Operation>>,
  docId: string,
  capabilities: Array<{ name: string; value: string }>,
): Promise<void> {
  await transport.send({
    v: 0,
    docId,
    payload: {
      case: 'hello',
      value: { capabilities, filters: [], maxLamport: 0n },
    },
  });
  await tick();
}

async function sendHelloAck(
  transport: DuplexTransport<SyncMessage<Operation>>,
  docId: string,
  capabilities: Array<{ name: string; value: string }>,
): Promise<void> {
  await transport.send({
    v: 0,
    docId,
    payload: {
      case: 'helloAck',
      value: {
        capabilities,
        acceptedFilters: [],
        rejectedFilters: [],
        maxLamport: 0n,
      },
    },
  });
  await tick();
}

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error('replica label must not be empty');
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

const replicas = {
  a: replicaFromLabel('a'),
  b: replicaFromLabel('b'),
  s: replicaFromLabel('s'),
};

const replicaHex = {
  a: bytesToHex(replicas.a),
  b: bytesToHex(replicas.b),
  s: bytesToHex(replicas.s),
};

function createTimedDuplex<M>(
  opts: { aToBDelayMs?: number; bToADelayMs?: number } = {},
): [DuplexTransport<M>, DuplexTransport<M>] {
  const aHandlers = new Set<(msg: M) => void>();
  const bHandlers = new Set<(msg: M) => void>();
  const aToBDelayMs = opts.aToBDelayMs ?? 0;
  const bToADelayMs = opts.bToADelayMs ?? 0;

  const a: DuplexTransport<M> = {
    async send(msg) {
      setTimeout(() => {
        for (const h of bHandlers) h(msg);
      }, aToBDelayMs);
    },
    onMessage(handler) {
      aHandlers.add(handler);
      return () => aHandlers.delete(handler);
    },
  };

  const b: DuplexTransport<M> = {
    async send(msg) {
      setTimeout(() => {
        for (const h of aHandlers) h(msg);
      }, bToADelayMs);
    },
    onMessage(handler) {
      bHandlers.add(handler);
      return () => bHandlers.delete(handler);
    },
  };

  return [a, b];
}

function createLoggedTimedDuplex<M>(
  opts: { aToBDelayMs?: number; bToADelayMs?: number } = {},
): [DuplexTransport<M>, DuplexTransport<M>, Array<{ dir: 'aToB' | 'bToA'; msg: M }>] {
  const aHandlers = new Set<(msg: M) => void>();
  const bHandlers = new Set<(msg: M) => void>();
  const log: Array<{ dir: 'aToB' | 'bToA'; msg: M }> = [];
  const aToBDelayMs = opts.aToBDelayMs ?? 0;
  const bToADelayMs = opts.bToADelayMs ?? 0;

  const a: DuplexTransport<M> = {
    async send(msg) {
      log.push({ dir: 'aToB', msg });
      setTimeout(() => {
        for (const h of bHandlers) h(msg);
      }, aToBDelayMs);
    },
    onMessage(handler) {
      aHandlers.add(handler);
      return () => aHandlers.delete(handler);
    },
  };

  const b: DuplexTransport<M> = {
    async send(msg) {
      log.push({ dir: 'bToA', msg });
      setTimeout(() => {
        for (const h of aHandlers) h(msg);
      }, bToADelayMs);
    },
    onMessage(handler) {
      bHandlers.add(handler);
      return () => bHandlers.delete(handler);
    },
  };

  return [a, b, log];
}

function createMacrotaskDuplex<M>(): [DuplexTransport<M>, DuplexTransport<M>] {
  return createTimedDuplex();
}

function createTerminalTransport<M>(): {
  transport: DuplexTransport<M>;
  sent: M[];
  messageHandlerCount: () => number;
  receive: (message: M) => void;
  terminate: (error?: unknown) => void;
} {
  const messageHandlers = new Set<(msg: M) => void>();
  const terminalHandlers = new Set<(error?: unknown) => void>();
  const sent: M[] = [];
  return {
    sent,
    messageHandlerCount: () => messageHandlers.size,
    transport: {
      async send(msg) {
        sent.push(msg);
      },
      onMessage(handler) {
        messageHandlers.add(handler);
        return () => messageHandlers.delete(handler);
      },
      onTerminal(handler) {
        terminalHandlers.add(handler);
        return () => terminalHandlers.delete(handler);
      },
    },
    receive(message) {
      for (const handler of messageHandlers) handler(message);
    },
    terminate(error) {
      for (const handler of terminalHandlers) handler(error);
      terminalHandlers.clear();
    },
  };
}

type OperationSyncPayload = SyncMessage<Operation>['payload'];
type OperationSyncCase = OperationSyncPayload['case'];
type OperationSyncPayloadFor<Case extends OperationSyncCase> = Extract<
  OperationSyncPayload,
  { case: Case }
>;
type OperationTransportControl = ReturnType<typeof createTerminalTransport<SyncMessage<Operation>>>;

function receiveSync<Case extends OperationSyncCase>(
  control: OperationTransportControl,
  docId: string,
  payload: OperationSyncPayloadFor<Case>,
): void {
  control.receive({ v: 0, docId, payload });
}

function sentValues<Case extends OperationSyncCase>(
  control: OperationTransportControl,
  payloadCase: Case,
): Array<OperationSyncPayloadFor<Case>['value']> {
  return control.sent.flatMap((message) =>
    message.payload.case === payloadCase
      ? [message.payload.value as OperationSyncPayloadFor<Case>['value']]
      : [],
  );
}

function receiveError(
  control: OperationTransportControl,
  docId: string,
  message: string,
  ids: { filterId?: string; subscriptionId?: string } = {},
): void {
  receiveSync(control, docId, {
    case: 'error',
    value: { code: ErrorCode.ERROR_CODE_UNSPECIFIED, message, ...ids },
  });
}

async function expectPending(promise: Promise<unknown>): Promise<void> {
  const state = await Promise.race([
    promise.then(
      () => 'settled',
      () => 'settled',
    ),
    tick().then(() => 'pending'),
  ]);
  expect(state).toBe('pending');
}

function createPeers(a: SyncBackend<Operation>, b: SyncBackend<Operation>) {
  return createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
  });
}

async function waitUntil(
  predicate: () => Promise<boolean> | boolean,
  opts: { timeoutMs?: number; intervalMs?: number; message?: string } = {},
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 2_000;
  const intervalMs = opts.intervalMs ?? 10;
  const start = Date.now();
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const ok = await predicate();
    if (ok) return;
    if (Date.now() - start > timeoutMs) {
      throw new Error(opts.message ?? `waitUntil timeout after ${timeoutMs}ms`);
    }
    await new Promise<void>((resolve) => setTimeout(resolve, intervalMs));
  }
}

const TRASH_HEX = 'ff'.repeat(16);

class MemoryBackend implements SyncBackend<Operation> {
  readonly docId: string;

  private maxLamportValue = 0n;
  private readonly opsByRefHex = new Map<string, { opRef: OpRef; op: Operation }>();

  constructor(docId: string) {
    this.docId = docId;
  }

  private opRefForOp(op: Operation): OpRef {
    const replicaHex = bytesToHex(op.meta.id.replica);
    return opRefFor(this.docId, replicaHex, op.meta.id.counter);
  }

  hasOp(replicaHex: string, counter: number): boolean {
    return Array.from(this.opsByRefHex.values()).some(
      (v) => bytesToHex(v.op.meta.id.replica) === replicaHex && v.op.meta.id.counter === counter,
    );
  }

  async maxLamport(): Promise<bigint> {
    return this.maxLamportValue;
  }

  async listOpRefs(filter: Filter): Promise<OpRef[]> {
    if ('all' in filter) {
      return Array.from(this.opsByRefHex.values(), (v) => v.opRef);
    }

    const targetParentHex = bytesToHex(filter.children.parent);

    // TODO(sync): `children(parent)` is defined in terms of the *canonical* tree state
    // (needs "old parent == P" for boundary-crossing moves). This naive implementation
    // replays/scans the entire local op log each time the filter is evaluated.
    //
    // Production backends should maintain a materialized parent/children index (or store
    // `old_parent` for move-like ops) so this doesn't become an O(total_ops) scan.
    const entries = Array.from(this.opsByRefHex.values()).sort((a, b) => {
      if (a.op.meta.lamport < b.op.meta.lamport) return -1;
      if (a.op.meta.lamport > b.op.meta.lamport) return 1;
      // Deterministic tie-breaker for the scan: opRef bytes.
      const ah = bytesToHex(a.opRef);
      const bh = bytesToHex(b.opRef);
      return ah < bh ? -1 : ah > bh ? 1 : 0;
    });

    const parentByNodeHex = new Map<string, string>();
    const latestPayloadByNodeHex = new Map<string, OpRef>();
    const relevantByHex = new Map<string, OpRef>();

    const add = (ref: OpRef) => {
      const hex = bytesToHex(ref);
      if (!relevantByHex.has(hex)) relevantByHex.set(hex, ref);
    };

    for (const { opRef, op } of entries) {
      switch (op.kind.type) {
        case 'insert': {
          const nodeHex = op.kind.node;
          const oldParentHex = parentByNodeHex.get(nodeHex);
          const newParentHex = op.kind.parent;

          if (oldParentHex === targetParentHex || newParentHex === targetParentHex) {
            add(opRef);
          }
          if (oldParentHex !== newParentHex && newParentHex === targetParentHex) {
            const payloadRef = latestPayloadByNodeHex.get(nodeHex);
            if (payloadRef) add(payloadRef);
          }

          parentByNodeHex.set(nodeHex, newParentHex);
          break;
        }
        case 'move': {
          const nodeHex = op.kind.node;
          const oldParentHex = parentByNodeHex.get(nodeHex);
          const newParentHex = op.kind.newParent;

          if (oldParentHex === targetParentHex || newParentHex === targetParentHex) {
            add(opRef);
          }
          if (oldParentHex !== newParentHex && newParentHex === targetParentHex) {
            const payloadRef = latestPayloadByNodeHex.get(nodeHex);
            if (payloadRef) add(payloadRef);
          }

          parentByNodeHex.set(nodeHex, newParentHex);
          break;
        }
        case 'delete':
        case 'tombstone': {
          const nodeHex = op.kind.node;
          const oldParentHex = parentByNodeHex.get(nodeHex);
          const newParentHex = TRASH_HEX;

          if (oldParentHex === targetParentHex || newParentHex === targetParentHex) {
            add(opRef);
          }

          parentByNodeHex.set(nodeHex, newParentHex);
          break;
        }
        case 'payload': {
          const nodeHex = op.kind.node;
          const parentHex = parentByNodeHex.get(nodeHex);
          if (parentHex === targetParentHex) add(opRef);
          latestPayloadByNodeHex.set(nodeHex, opRef);
          break;
        }
        default: {
          const _exhaustive: never = op.kind;
          throw new Error(`unknown op kind: ${String((_exhaustive as any)?.type)}`);
        }
      }
    }

    return Array.from(relevantByHex.values());
  }

  async getOpsByOpRefs(opRefs: OpRef[]): Promise<Operation[]> {
    return opRefs.map((r) => {
      const stored = this.opsByRefHex.get(bytesToHex(r));
      if (!stored) throw new Error('opRef missing locally');
      return stored.op;
    });
  }

  async applyOps(ops: Operation[]): Promise<void> {
    for (const op of ops) {
      const opRef = this.opRefForOp(op);
      const opRefHex = bytesToHex(opRef);
      if (this.opsByRefHex.has(opRefHex)) continue;

      this.opsByRefHex.set(opRefHex, { opRef, op });
      const lamport = BigInt(op.meta.lamport);
      if (lamport > this.maxLamportValue) this.maxLamportValue = lamport;
    }
  }
}

class FailingApplyBackend extends MemoryBackend {
  override async applyOps(_ops: Operation[]): Promise<void> {
    throw new Error('apply failed');
  }
}

class DelayedApplyBackend extends MemoryBackend {
  constructor(
    docId: string,
    private readonly delayMs: number,
  ) {
    super(docId);
  }

  override async applyOps(ops: Operation[]): Promise<void> {
    await new Promise<void>((resolve) => setTimeout(resolve, this.delayMs));
    await super.applyOps(ops);
  }
}

class CountingListBackend extends MemoryBackend {
  listOpRefsCalls = 0;

  override async listOpRefs(filter: Filter): Promise<OpRef[]> {
    this.listOpRefsCalls += 1;
    return super.listOpRefs(filter);
  }
}

class BlockingFirstApplyBackend extends MemoryBackend {
  applyOpsCalls = 0;
  private releaseFirstApply!: () => void;
  private readonly firstApplyGate = new Promise<void>((resolve) => {
    this.releaseFirstApply = resolve;
  });

  release(): void {
    this.releaseFirstApply();
  }

  override async applyOps(ops: Operation[]): Promise<void> {
    this.applyOpsCalls += 1;
    if (this.applyOpsCalls === 1) await this.firstApplyGate;
    await super.applyOps(ops);
  }
}

test('transport close rejects only its own syncOnce handshake', async () => {
  const backend = new MemoryBackend('doc-transport-scoped-close');
  const transportA = createTerminalTransport<SyncMessage<Operation>>();
  const transportB = createTerminalTransport<SyncMessage<Operation>>();
  const peer = new SyncPeer(backend);
  const detachA = peer.attach(transportA.transport);
  const detachB = peer.attach(transportB.transport);
  const syncA = peer.syncOnce(transportA.transport, { all: {} });
  const syncB = peer.syncOnce(transportB.transport, { all: {} });
  const rejectedA = expect(syncA).rejects.toThrow('transport A closed');

  await waitUntil(() => transportA.sent.length > 0 && transportB.sent.length > 0, {
    message: 'expected both syncOnce calls to send Hello',
  });
  transportA.terminate(new Error('transport A closed'));

  await rejectedA;
  expect(transportA.messageHandlerCount()).toBe(0);
  expect(transportB.messageHandlerCount()).toBe(1);

  const rejectedB = expect(syncB).rejects.toThrow('transport B closed');
  transportB.terminate(new Error('transport B closed'));
  await rejectedB;
  detachA();
  detachB();
});

test('riblt status only advances the session owned by its transport', async () => {
  const docId = 'doc-transport-scoped-riblt-status';
  const backend = new MemoryBackend(docId);
  const transportA = createTerminalTransport<SyncMessage<Operation>>();
  const transportB = createTerminalTransport<SyncMessage<Operation>>();
  const peer = new SyncPeer(backend);
  const detachA = peer.attach(transportA.transport);
  const detachB = peer.attach(transportB.transport);
  const sync = peer.syncOnce(transportA.transport, { all: {} });
  void sync.catch(() => {});

  try {
    await waitUntil(() => sentValues(transportA, 'hello').length === 1);
    const filterId = sentValues(transportA, 'hello')[0]?.filters[0]?.id;
    if (!filterId) throw new Error('expected Hello filter id');

    receiveSync(transportB, docId, {
      case: 'helloAck',
      value: {
        capabilities: [],
        acceptedFilters: [],
        rejectedFilters: [
          {
            id: filterId,
            reason: ErrorCode.ERROR_CODE_UNSPECIFIED,
            message: 'wrong transport rejection',
          },
        ],
        maxLamport: 0n,
      },
    });
    await tick();
    expect((peer as any).initiatorSessions.has(filterId)).toBe(true);

    receiveSync(transportB, docId, {
      case: 'helloAck',
      value: {
        capabilities: [],
        acceptedFilters: [filterId],
        rejectedFilters: [],
        maxLamport: 0n,
      },
    });
    await tick();
    expect(transportA.sent.some((msg) => msg.payload.case === 'ribltCodewords')).toBe(false);

    receiveSync(transportA, docId, {
      case: 'helloAck',
      value: {
        capabilities: [],
        acceptedFilters: [filterId],
        rejectedFilters: [],
        maxLamport: 0n,
      },
    });
    await waitUntil(() => transportA.sent.some((msg) => msg.payload.case === 'ribltCodewords'));

    const decodedStatus: OperationSyncPayloadFor<'ribltStatus'> = {
      case: 'ribltStatus',
      value: {
        filterId,
        round: 0,
        payload: {
          case: 'decoded',
          value: { senderMissing: [], receiverMissing: [], codewordsReceived: 0n },
        },
      },
    };
    receiveSync(transportB, docId, decodedStatus);
    await tick();
    expect(transportA.sent.some((msg) => msg.payload.case === 'opsBatch')).toBe(false);

    receiveSync(transportA, docId, decodedStatus);
    await waitUntil(() => transportA.sent.some((msg) => msg.payload.case === 'opsBatch'));
    const doneBatch: OperationSyncPayloadFor<'opsBatch'> = {
      case: 'opsBatch',
      value: { filterId, ops: [], done: true },
    };
    receiveSync(transportB, docId, doneBatch);
    await expectPending(sync);

    receiveSync(transportA, docId, doneBatch);
    await sync;
  } finally {
    detachA();
    detachB();
  }
});

test('subscription acknowledgements and errors are scoped to their transport', async () => {
  const docId = 'doc-transport-scoped-subscription';
  const backend = new MemoryBackend(docId);
  const transportA = createTerminalTransport<SyncMessage<Operation>>();
  const transportB = createTerminalTransport<SyncMessage<Operation>>();
  const verifiedPurposes: string[] = [];
  const peer = new SyncPeer(backend, {
    requireAuthForFilters: false,
    auth: {
      verifyOps: async (_ops, _auth, ctx) => {
        verifiedPurposes.push(ctx.purpose);
      },
    },
  });
  const detachA = peer.attach(transportA.transport);
  const detachB = peer.attach(transportB.transport);
  const sub = peer.subscribe(
    transportA.transport,
    { all: {} },
    { immediate: false, intervalMs: 0 },
  );
  void sub.done.catch(() => {});

  try {
    await waitUntil(() => sentValues(transportA, 'subscribe').length === 1);
    const subscriptionId = sentValues(transportA, 'subscribe')[0]!.subscriptionId;

    receiveSync(transportB, docId, {
      case: 'subscribeAck',
      value: { subscriptionId, currentLamport: 0n },
    });
    receiveError(transportB, docId, 'wrong transport subscription error', { subscriptionId });
    await expectPending(sub.ready);

    receiveSync(transportA, docId, {
      case: 'subscribeAck',
      value: { subscriptionId, currentLamport: 0n },
    });
    await sub.ready;
    const unrelatedSync = peer.syncOnce(transportA.transport, { all: {} });
    void unrelatedSync.catch(() => {});
    await waitUntil(() => sentValues(transportA, 'hello').length === 1);

    receiveSync(transportB, docId, {
      case: 'opsBatch',
      value: { filterId: subscriptionId, ops: [], done: true },
    });
    receiveSync(transportA, docId, {
      case: 'opsBatch',
      value: { filterId: subscriptionId, ops: [], done: true },
    });
    await waitUntil(() => verifiedPurposes.length === 2);
    expect(verifiedPurposes).toEqual(['reconcile', 'subscribe']);

    receiveError(transportB, docId, 'wrong transport live error', { subscriptionId });
    await expectPending(sub.done);

    const ownError = expect(sub.done).rejects.toThrow('own transport subscription error');
    receiveError(transportA, docId, 'own transport subscription error', { subscriptionId });
    await ownError;
    await expectPending(unrelatedSync);
  } finally {
    sub.stop();
    await sub.done.catch(() => {});
    detachA();
    detachB();
  }
});

test('unsubscribe only removes responder state owned by its transport', async () => {
  const docId = 'doc-transport-scoped-unsubscribe';
  const subscriptionId = 'sub_transport_a';
  const backend = new MemoryBackend(docId);
  const transportA = createTerminalTransport<SyncMessage<Operation>>();
  const transportB = createTerminalTransport<SyncMessage<Operation>>();
  const peer = new SyncPeer(backend);
  const detachA = peer.attach(transportA.transport);
  const detachB = peer.attach(transportB.transport);

  try {
    const subscribe: OperationSyncPayloadFor<'subscribe'> = {
      case: 'subscribe',
      value: { subscriptionId, filter: { all: {} } },
    };
    receiveSync(transportA, docId, subscribe);
    receiveSync(transportB, docId, subscribe);
    await waitUntil(() => (peer as any).responderSubscriptions.get(subscriptionId)?.size === 2);

    receiveSync(transportB, docId, {
      case: 'unsubscribe',
      value: { subscriptionId },
    });
    await waitUntil(() => (peer as any).responderSubscriptions.get(subscriptionId)?.size === 1);
    expect(
      (peer as any).responderSubscriptions.get(subscriptionId)?.has(transportA.transport),
    ).toBe(true);

    receiveSync(transportA, docId, {
      case: 'unsubscribe',
      value: { subscriptionId },
    });
    await waitUntil(() => !(peer as any).responderSubscriptions.has(subscriptionId));
  } finally {
    detachA();
    detachB();
  }
});

test('identical responder filter ids remain independent across transports', async () => {
  const docId = 'doc-transport-scoped-responder-filter';
  const filterId = 'shared_filter_id';
  const backend = new MemoryBackend(docId);
  const transportA = createTerminalTransport<SyncMessage<Operation>>();
  const transportB = createTerminalTransport<SyncMessage<Operation>>();
  const peer = new SyncPeer(backend);
  const detachA = peer.attach(transportA.transport);
  const detachB = peer.attach(transportB.transport);
  const hello: OperationSyncPayloadFor<'hello'> = {
    case: 'hello',
    value: {
      capabilities: [],
      filters: [{ id: filterId, filter: { all: {} } }],
      maxLamport: 0n,
    },
  };

  try {
    receiveSync(transportA, docId, hello);
    receiveSync(transportB, docId, hello);
    await waitUntil(() => (peer as any).responderSessions.get(filterId)?.size === 2);

    const outOfOrder: OperationSyncPayloadFor<'ribltCodewords'> = {
      case: 'ribltCodewords',
      value: { filterId, round: 0, startIndex: 1n, codewords: [] },
    };
    receiveSync(transportB, docId, outOfOrder);
    await waitUntil(() => (peer as any).responderSessions.get(filterId)?.size === 1);
    expect((peer as any).responderSessions.get(filterId)?.has(transportA.transport)).toBe(true);
    expect(transportA.sent.some((msg) => msg.payload.case === 'ribltStatus')).toBe(false);
    expect(transportB.sent.some((msg) => msg.payload.case === 'ribltStatus')).toBe(true);

    receiveSync(transportA, docId, outOfOrder);
    await waitUntil(() => !(peer as any).responderSessions.has(filterId));
    expect(transportA.sent.some((msg) => msg.payload.case === 'ribltStatus')).toBe(true);
  } finally {
    detachA();
    detachB();
  }
});

test('direct-upload acknowledgements are scoped by transport and filter id', async () => {
  const docId = 'doc-transport-scoped-upload-ack';
  const filterId = 'shared_upload_filter';
  const backend = new MemoryBackend(docId);
  const transportA = createTerminalTransport<SyncMessage<Operation>>();
  const transportB = createTerminalTransport<SyncMessage<Operation>>();
  const peer = new SyncPeer(backend);
  const detachA = peer.attach(transportA.transport);
  const detachB = peer.attach(transportB.transport);
  const hello: OperationSyncPayloadFor<'hello'> = {
    case: 'hello',
    value: {
      capabilities: [{ name: 'treecrdt.sync.direct_send_empty_receiver.v1', value: '1' }],
      filters: [{ id: filterId, filter: { all: {} } }],
      maxLamport: 0n,
    },
  };
  const doneBatch: OperationSyncPayloadFor<'opsBatch'> = {
    case: 'opsBatch',
    value: { filterId, ops: [], done: true },
  };

  try {
    receiveSync(transportA, docId, hello);
    receiveSync(transportB, docId, hello);
    await waitUntil(() => (peer as any).responderAwaitingUploadAcks.get(filterId)?.size === 2);

    receiveSync(transportA, docId, doneBatch);
    await waitUntil(() => transportA.sent.some((msg) => msg.payload.case === 'opsBatch'));
    expect(transportB.sent.some((msg) => msg.payload.case === 'opsBatch')).toBe(false);
    expect((peer as any).responderAwaitingUploadAcks.get(filterId)?.has(transportB.transport)).toBe(
      true,
    );

    receiveSync(transportB, docId, doneBatch);
    await waitUntil(() => transportB.sent.some((msg) => msg.payload.case === 'opsBatch'));
    expect((peer as any).responderAwaitingUploadAcks.has(filterId)).toBe(false);
  } finally {
    detachA();
    detachB();
  }
});

test('ops batches with the same filter id queue independently across transports', async () => {
  const docId = 'doc-transport-scoped-ops-queue';
  const backend = new BlockingFirstApplyBackend(docId);
  const transportA = createTerminalTransport<SyncMessage<Operation>>();
  const transportB = createTerminalTransport<SyncMessage<Operation>>();
  const peer = new SyncPeer(backend);
  const detachA = peer.attach(transportA.transport);
  const detachB = peer.attach(transportB.transport);
  const batch: OperationSyncPayloadFor<'opsBatch'> = {
    case: 'opsBatch',
    value: { filterId: 'shared_ops_stream', ops: [], done: false },
  };

  try {
    receiveSync(transportA, docId, batch);
    await waitUntil(() => backend.applyOpsCalls === 1);
    receiveSync(transportB, docId, batch);
    await waitUntil(() => backend.applyOpsCalls === 2, {
      message: 'expected the second transport to use an independent ops queue',
    });
  } finally {
    backend.release();
    await waitUntil(() => (peer as any).opsBatchQueues.size === 0);
    detachA();
    detachB();
  }
});

test('addressed and global errors only reject sessions owned by their transport', async () => {
  const docId = 'doc-transport-scoped-errors';
  const backend = new MemoryBackend(docId);
  const transportA = createTerminalTransport<SyncMessage<Operation>>();
  const transportB = createTerminalTransport<SyncMessage<Operation>>();
  const peer = new SyncPeer(backend);
  const detachA = peer.attach(transportA.transport);
  const detachB = peer.attach(transportB.transport);
  const syncA1 = peer.syncOnce(transportA.transport, { all: {} });
  const syncB = peer.syncOnce(transportB.transport, { all: {} });
  void syncA1.catch(() => {});
  void syncB.catch(() => {});

  try {
    await waitUntil(
      () =>
        sentValues(transportA, 'hello').length === 1 &&
        sentValues(transportB, 'hello').length === 1,
    );
    const filterA1 = sentValues(transportA, 'hello')[0]?.filters[0]?.id;
    const filterB = sentValues(transportB, 'hello')[0]?.filters[0]?.id;
    if (!filterA1 || !filterB) throw new Error('expected Hello filter ids');

    receiveError(transportB, docId, 'wrong transport addressed error', { filterId: filterA1 });
    await tick();
    expect((peer as any).initiatorSessions.has(filterA1)).toBe(true);

    const addressedError = expect(syncA1).rejects.toThrow('own transport addressed error');
    receiveError(transportA, docId, 'own transport addressed error', { filterId: filterA1 });
    await addressedError;
    expect((peer as any).initiatorSessions.has(filterB)).toBe(true);

    const syncA2 = peer.syncOnce(transportA.transport, { all: {} });
    void syncA2.catch(() => {});
    await waitUntil(() => sentValues(transportA, 'hello').length === 2);
    const globalAError = expect(syncA2).rejects.toThrow('global transport A error');
    receiveError(transportA, docId, 'global transport A error');
    await globalAError;
    expect((peer as any).initiatorSessions.has(filterB)).toBe(true);

    const globalBError = expect(syncB).rejects.toThrow('global transport B error');
    receiveError(transportB, docId, 'global transport B error');
    await globalBError;
    expect((peer as any).initiatorSessions.size).toBe(0);
  } finally {
    detachA();
    detachB();
  }
});

test('syncOnce does not starve macrotask transports', async () => {
  const docId = 'doc-sync-macrotask';
  const root = '0'.repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  await a.applyOps([
    makeOp(replicas.a, 1, 1, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    }),
  ]);

  const [wa, wb] = createMacrotaskDuplex<Uint8Array>();
  const ta = wrapDuplexTransportWithCodec(wa, treecrdtSyncV0ProtobufCodec);
  const tb = wrapDuplexTransportWithCodec(wb, treecrdtSyncV0ProtobufCodec);
  const pa = new SyncPeer(a);
  const pb = new SyncPeer(b);
  pa.attach(ta);
  pb.attach(tb);

  await pa.syncOnce(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });

  await waitUntil(() => b.hasOp(replicaHex.a, 1), {
    message: 'expected b to receive a:1 via macrotask duplex',
  });
});

test('syncOnce paces outbound codewords until delayed ribltStatus arrives', async () => {
  const docId = 'doc-sync-delayed-status';
  const root = '0'.repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  await a.applyOps([
    makeOp(replicas.a, 1, 1, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    }),
  ]);

  const [wa, wb] = createTimedDuplex<Uint8Array>({ bToADelayMs: 40 });
  const ta = wrapDuplexTransportWithCodec(wa, treecrdtSyncV0ProtobufCodec);
  const tb = wrapDuplexTransportWithCodec(wb, treecrdtSyncV0ProtobufCodec);
  const pa = new SyncPeer(a);
  const pb = new SyncPeer(b);
  pa.attach(ta);
  pb.attach(tb);

  await pa.syncOnce(ta, { all: {} }, { maxCodewords: 1_024, codewordsPerMessage: 256 });

  await waitUntil(() => b.hasOp(replicaHex.a, 1), {
    message: 'expected b to receive a:1 after delayed ribltStatus',
  });
});

test('syncOnce waits for responder to apply uploaded ops before resolving', async () => {
  const docId = 'doc-sync-upload-ack';
  const root = '0'.repeat(32);

  const a = new MemoryBackend(docId);
  const b = new DelayedApplyBackend(docId, 20);

  await a.applyOps([
    makeOp(replicas.a, 1, 1, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    }),
    makeOp(replicas.a, 2, 2, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(2),
      orderKey: orderKeyFromPosition(1),
    }),
    makeOp(replicas.a, 3, 3, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(3),
      orderKey: orderKeyFromPosition(2),
    }),
  ]);

  const [wa, wb] = createMacrotaskDuplex<Uint8Array>();
  const ta = wrapDuplexTransportWithCodec(wa, treecrdtSyncV0ProtobufCodec);
  const tb = wrapDuplexTransportWithCodec(wb, treecrdtSyncV0ProtobufCodec);
  const pa = new SyncPeer(a);
  const pb = new SyncPeer(b);
  pa.attach(ta);
  pb.attach(tb);

  await pa.syncOnce(
    ta,
    { all: {} },
    { maxCodewords: 10_000, codewordsPerMessage: 256, maxOpsPerBatch: 1 },
  );

  expect(b.hasOp(replicaHex.a, 1)).toBe(true);
  expect(b.hasOp(replicaHex.a, 2)).toBe(true);
  expect(b.hasOp(replicaHex.a, 3)).toBe(true);
});

test('pushOps uploads direct ops without reconcile roundtrips', async () => {
  const docId = 'doc-push-direct';
  const root = '0'.repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);
  const op = makeOp(replicas.a, 1, 1, {
    type: 'insert',
    parent: root,
    node: nodeIdFromInt(1),
    orderKey: orderKeyFromPosition(0),
  });

  const [transportA, transportB, wire] = createLoggedTimedDuplex<SyncMessage<Operation>>();
  const peerA = new SyncPeer(a, { maxOpsPerBatch: 1 });
  const peerB = new SyncPeer(b, { maxOpsPerBatch: 1 });
  const detachA = peerA.attach(transportA);
  const detachB = peerB.attach(transportB);

  try {
    await peerA.pushOps(transportA, [op]);

    await waitUntil(() => b.hasOp(replicaHex.a, 1), {
      message: 'expected direct push to apply on receiver',
    });

    const serverCases = wire
      .filter((entry) => entry.dir === 'aToB')
      .map((entry) => entry.msg.payload.case);
    expect(serverCases).toEqual(['opsBatch']);
  } finally {
    detachA();
    detachB();
  }
});

test('pushOps refreshes withdrawn reader authority and shares a concurrent refresh', async () => {
  const docId = 'doc-push-reader-refresh';
  const root = '0'.repeat(32);
  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);
  let advertiseReader = true;
  const op = (counter: number) =>
    makeOp(replicas.a, counter, counter, {
      type: 'insert' as const,
      parent: root,
      node: nodeIdFromInt(counter),
      orderKey: orderKeyFromPosition(counter - 1),
    });

  const [transportA, transportB, wire] = createLoggedTimedDuplex<SyncMessage<Operation>>({
    bToADelayMs: 25,
  });
  const peerA = new SyncPeer(a, {
    auth: {
      helloCapabilities: async () => [{ name: 'auth.capability', value: 'sender' }],
      filterOutgoingOps: async (ops, ctx) => {
        expect(ctx.filter).toEqual({ all: {} });
        const canRead = ctx.capabilities.some(
          (capability) => capability.name === 'auth.capability' && capability.value === 'reader',
        );
        return ops.map(() => canRead);
      },
    },
  });
  const peerB = new SyncPeer(b, {
    auth: {
      onHello: async () => (advertiseReader ? [{ name: 'auth.capability', value: 'reader' }] : []),
    },
  });
  const detachA = peerA.attach(transportA);
  const detachB = peerB.attach(transportB);

  try {
    await peerA.pushOps(transportA, [op(1)]);
    await waitUntil(() => b.hasOp(replicaHex.a, 1));

    advertiseReader = false;
    const hellosBefore = wire.filter(
      (entry) => entry.dir === 'aToB' && entry.msg.payload.case === 'hello',
    ).length;
    await Promise.all([peerA.pushOps(transportA, [op(2)]), peerA.pushOps(transportA, [op(3)])]);
    await tick();

    const hellosAfter = wire.filter(
      (entry) => entry.dir === 'aToB' && entry.msg.payload.case === 'hello',
    ).length;
    expect(hellosAfter - hellosBefore).toBe(1);
    expect(b.hasOp(replicaHex.a, 2)).toBe(false);
    expect(b.hasOp(replicaHex.a, 3)).toBe(false);
  } finally {
    detachA();
    detachB();
  }
});

test('pushOps retries the same capability snapshot after remote and local rejection', async () => {
  const docId = 'doc-push-capability-retry';
  const root = '0'.repeat(32);
  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);
  let helloAttempt = 0;

  const [transportA, transportB] = createTimedDuplex<SyncMessage<Operation>>();
  const peerA = new SyncPeer(a, {
    auth: {
      helloCapabilities: async () => [{ name: 'auth.capability', value: 'sender' }],
      onHelloAck: async () => {
        if (helloAttempt === 2) throw new Error('local rejected ack');
      },
      filterOutgoingOps: async (ops, ctx) =>
        ops.map(() =>
          ctx.capabilities.some(
            (capability) => capability.name === 'auth.capability' && capability.value === 'reader',
          ),
        ),
    },
  });
  const peerB = new SyncPeer(b, {
    auth: {
      onHello: async () => {
        helloAttempt += 1;
        if (helloAttempt === 1) throw new Error('remote rejected hello');
        return [{ name: 'auth.capability', value: 'reader' }];
      },
    },
  });
  const detachA = peerA.attach(transportA);
  const detachB = peerB.attach(transportB);
  const makePushOp = (counter: number) =>
    makeOp(replicas.a, counter, counter, {
      type: 'insert' as const,
      parent: root,
      node: nodeIdFromInt(counter),
      orderKey: orderKeyFromPosition(counter - 1),
    });

  try {
    await expect(peerA.pushOps(transportA, [makePushOp(1)])).rejects.toThrow(
      /remote rejected hello/,
    );
    await expect(peerA.pushOps(transportA, [makePushOp(2)])).rejects.toThrow(/local rejected ack/);
    await peerA.pushOps(transportA, [makePushOp(3)]);
    await waitUntil(() => b.hasOp(replicaHex.a, 3));

    expect(helloAttempt).toBe(3);
    expect(b.hasOp(replicaHex.a, 1)).toBe(false);
    expect(b.hasOp(replicaHex.a, 2)).toBe(false);
  } finally {
    detachA();
    detachB();
  }
});

test('rejected and out-of-order capability snapshots stay hidden from outgoing auth', async () => {
  const docId = 'doc-capability-snapshot-validation';
  const root = '0'.repeat(32);
  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);
  let releaseSlowHello!: () => void;
  const slowHello = new Promise<void>((resolve) => {
    releaseSlowHello = resolve;
  });
  let releaseSlowAck!: () => void;
  const slowAck = new Promise<void>((resolve) => {
    releaseSlowAck = resolve;
  });
  const seenCapabilities: Array<Array<{ name: string; value: string }>> = [];

  const [transportA, transportB, wire] = createLoggedTimedDuplex<SyncMessage<Operation>>();
  const peerA = new SyncPeer(a, {
    auth: {
      onHello: async (hello) => {
        const value = hello.capabilities[0]?.value;
        if (value === 'bad-hello') throw new Error('rejected hello snapshot');
        if (value === 'slow-bad-hello') {
          await slowHello;
          throw new Error('stale rejected hello snapshot');
        }
        return [];
      },
      onHelloAck: async (ack) => {
        const value = ack.capabilities[0]?.value;
        if (value === 'bad-ack') {
          throw new Error('rejected ack snapshot');
        }
        if (value === 'slow-bad-ack') {
          await slowAck;
          throw new Error('stale rejected ack snapshot');
        }
      },
      filterOutgoingOps: async (ops, ctx) => {
        seenCapabilities.push([...ctx.capabilities]);
        return ops.map(() => false);
      },
    },
  });
  const peerB = new SyncPeer(b);
  const detachA = peerA.attach(transportA);
  const detachB = peerB.attach(transportB);
  const op = (counter: number) =>
    makeOp(replicas.a, counter, counter, {
      type: 'insert' as const,
      parent: root,
      node: nodeIdFromInt(counter),
      orderKey: orderKeyFromPosition(counter - 1),
    });

  try {
    await sendHello(transportB, docId, [{ name: 'auth.capability', value: 'reader' }]);
    await sendHello(transportB, docId, [{ name: 'auth.capability', value: 'bad-hello' }]);
    await peerA.pushOps(transportA, [op(1)]);

    await sendHelloAck(transportB, docId, [{ name: 'auth.capability', value: 'reader' }]);
    await sendHelloAck(transportB, docId, [{ name: 'auth.capability', value: 'bad-ack' }]);
    await peerA.pushOps(transportA, [op(2)]);

    const errorsBeforeStaleSnapshots = wire.filter(
      (entry) => entry.dir === 'aToB' && entry.msg.payload.case === 'error',
    ).length;
    await sendHello(transportB, docId, [{ name: 'auth.capability', value: 'slow-bad-hello' }]);
    await sendHello(transportB, docId, []);
    releaseSlowHello();
    await tick();
    await sendHelloAck(transportB, docId, [{ name: 'auth.capability', value: 'slow-bad-ack' }]);
    await sendHelloAck(transportB, docId, []);
    releaseSlowAck();
    await tick();
    await peerA.pushOps(transportA, [op(3)]);

    expect(seenCapabilities).toEqual([[], [], []]);
    expect(
      wire.filter((entry) => entry.dir === 'aToB' && entry.msg.payload.case === 'error'),
    ).toHaveLength(errorsBeforeStaleSnapshots);
    expect(b.hasOp(replicaHex.a, 1)).toBe(false);
    expect(b.hasOp(replicaHex.a, 2)).toBe(false);
    expect(b.hasOp(replicaHex.a, 3)).toBe(false);
  } finally {
    releaseSlowHello();
    releaseSlowAck();
    detachA();
    detachB();
  }
});

test('pushOps refreshes replay capabilities before uploading newly authorized ops', async () => {
  const docId = 'doc-push-capability-refresh';
  const root = '0'.repeat(32);
  const proofRef = new Uint8Array([0x12, 0x34, 0x56, 0x78]);
  const replayCapValue = bytesToHex(proofRef);
  const knownReplayCaps = new Set<string>();
  let senderHelloCaps: Array<{ name: string; value: string }> = [];

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);
  const op = makeOp(replicas.a, 1, 1, {
    type: 'insert',
    parent: root,
    node: nodeIdFromInt(11),
    orderKey: orderKeyFromPosition(0),
  });

  const [transportA, transportB, wire] = createLoggedTimedDuplex<SyncMessage<Operation>>();
  const peerA = new SyncPeer(a, {
    auth: {
      helloCapabilities: async () => senderHelloCaps,
      signOps: async (ops) => ops.map(() => ({ sig: new Uint8Array([1]), proofRef })),
    },
    maxOpsPerBatch: 1,
  });
  const peerB = new SyncPeer(b, {
    auth: {
      helloCapabilities: async () => [{ name: 'auth.capability', value: 'receiver-token' }],
      onHello: async (hello) => {
        for (const cap of hello.capabilities) {
          if (cap.name === 'auth.capability.replay') knownReplayCaps.add(cap.value);
        }
        return [{ name: 'auth.capability', value: 'receiver-token' }];
      },
      verifyOps: async (_ops, auth) => {
        if (!auth) throw new Error('expected auth on direct push');
        for (const entry of auth) {
          if (!entry?.proofRef) throw new Error('expected proofRef on direct push');
          if (!knownReplayCaps.has(bytesToHex(entry.proofRef))) {
            throw new Error('missing replay capability for pushed proofRef');
          }
        }
      },
    },
    maxOpsPerBatch: 1,
  });

  const detachA = peerA.attach(transportA);
  const detachB = peerB.attach(transportB);

  try {
    senderHelloCaps = [{ name: 'auth.capability.replay', value: replayCapValue }];
    await peerA.pushOps(transportA, [op]);

    await waitUntil(() => b.hasOp(replicaHex.a, 1), {
      message: 'expected direct push to apply after capability refresh',
    });
    expect(knownReplayCaps).toEqual(new Set([replayCapValue]));

    const wireCases = wire.map((entry) => `${entry.dir}:${entry.msg.payload.case}`);
    expect(wireCases).toContain('aToB:hello');
    expect(wireCases).toContain('bToA:helloAck');
    expect(wireCases).toContain('aToB:opsBatch');
    expect(wireCases.indexOf('aToB:hello')).toBeLessThan(wireCases.indexOf('aToB:opsBatch'));
    expect(wireCases.indexOf('bToA:helloAck')).toBeLessThan(wireCases.indexOf('aToB:opsBatch'));
  } finally {
    detachA();
    detachB();
  }
});

test('syncOnce protobuf roundtrips ribltStatus.more', () => {
  const msg = {
    v: 0,
    docId: 'doc-sync-more-codec',
    payload: {
      case: 'ribltStatus' as const,
      value: {
        filterId: 'f_more',
        round: 7,
        payload: {
          case: 'more' as const,
          value: { codewordsReceived: 9n, credits: 2 },
        },
      },
    },
  };

  expect(treecrdtSyncV0ProtobufCodec.decode(treecrdtSyncV0ProtobufCodec.encode(msg))).toEqual(msg);
});

test('syncOnce waits for ribltStatus.more before sending another codeword batch', async () => {
  const docId = 'doc-sync-more-flow-control';
  const root = '0'.repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const commonOp = makeOp(replicas.b, 1, 1, {
    type: 'insert',
    parent: root,
    node: nodeIdFromInt(100),
    orderKey: orderKeyFromPosition(100),
  });
  await a.applyOps([commonOp]);
  await b.applyOps([commonOp]);

  const ops: Operation[] = [];
  for (let i = 1; i <= 12; i += 1) {
    ops.push(
      makeOp(replicas.a, i, i, {
        type: 'insert',
        parent: root,
        node: nodeIdFromInt(i),
        orderKey: orderKeyFromPosition(i - 1),
      }),
    );
  }
  await a.applyOps(ops);

  const [ta, tb, log] = createLoggedTimedDuplex<SyncMessage<Operation>>({ bToADelayMs: 25 });
  const pa = new SyncPeer(a);
  const pb = new SyncPeer(b);
  pa.attach(ta);
  pb.attach(tb);

  await pa.syncOnce(ta, { all: {} }, { maxCodewords: 4_096, codewordsPerMessage: 1 });

  await waitUntil(() => b.hasOp(replicaHex.a, 12), {
    message: 'expected b to receive all ops after ribltStatus.more flow control',
  });

  const wire = log.map((entry) => `${entry.dir}:${entry.msg.payload.case}`);
  let sawMore = false;
  for (let i = 1; i < wire.length; i += 1) {
    if (wire[i - 1] === 'bToA:ribltStatus') {
      const status =
        log[i - 1]!.msg.payload.case === 'ribltStatus' ? log[i - 1]!.msg.payload.value : null;
      if (status?.payload.case === 'more') sawMore = true;
    }
    expect(!(wire[i - 1] === 'aToB:ribltCodewords' && wire[i] === 'aToB:ribltCodewords')).toBe(
      true,
    );
  }
  expect(sawMore).toBe(true);
});

test('syncOnce can direct-send a small clean-slate scope without riblt codewords', async () => {
  const docId = 'doc-sync-direct-send-small-scope';
  const root = '0'.repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  await b.applyOps([
    makeOp(replicas.b, 1, 1, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    }),
    makeOp(replicas.b, 2, 2, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(2),
      orderKey: orderKeyFromPosition(1),
    }),
  ]);

  const [ta, tb, log] = createLoggedTimedDuplex<SyncMessage<Operation>>();
  const pa = new SyncPeer(a, { directSendThreshold: 8 });
  const pb = new SyncPeer(b, { directSendThreshold: 8 });
  pa.attach(ta);
  pb.attach(tb);

  await pa.syncOnce(
    ta,
    { children: { parent: nodeIdToBytes16(root) } },
    {
      maxCodewords: 4_096,
      codewordsPerMessage: 16,
    },
  );

  await waitUntil(() => a.hasOp(replicaHex.b, 2), {
    message: 'expected clean-slate initiator to receive direct-sent scope',
  });

  const wire = log.map((entry) => `${entry.dir}:${entry.msg.payload.case}`);
  expect(wire).toContain('aToB:hello');
  expect(wire).toContain('bToA:helloAck');
  expect(wire).toContain('bToA:opsBatch');
  expect(wire).not.toContain('aToB:ribltCodewords');
  expect(wire).not.toContain('bToA:ribltStatus');
});

test('syncOnce can direct-send a clean-slate upload to an empty receiver without riblt codewords', async () => {
  const docId = 'doc-sync-direct-send-empty-receiver';
  const root = '0'.repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  await a.applyOps([
    makeOp(replicas.a, 1, 1, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    }),
    makeOp(replicas.a, 2, 2, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(2),
      orderKey: orderKeyFromPosition(1),
    }),
  ]);

  const [ta, tb, log] = createLoggedTimedDuplex<SyncMessage<Operation>>();
  const pa = new SyncPeer(a);
  const pb = new SyncPeer(b);
  pa.attach(ta);
  pb.attach(tb);

  await pa.syncOnce(
    ta,
    { all: {} },
    {
      maxCodewords: 4_096,
      codewordsPerMessage: 16,
      maxOpsPerBatch: 1,
    },
  );

  await waitUntil(() => b.hasOp(replicaHex.a, 2), {
    message: 'expected empty receiver to receive direct-sent upload',
  });

  const wire = log.map((entry) => `${entry.dir}:${entry.msg.payload.case}`);
  expect(wire).toContain('aToB:hello');
  expect(wire).toContain('bToA:helloAck');
  expect(wire).toContain('aToB:opsBatch');
  expect(wire).toContain('bToA:opsBatch');
  expect(wire).not.toContain('aToB:ribltCodewords');
  expect(wire).not.toContain('bToA:ribltStatus');
});

test('Hello rejects an all-filter payload projection before exposing RIBLT state', async () => {
  const docId = 'doc-all-filter-payload-preflight';
  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);
  await b.applyOps([
    makeOp(replicas.b, 1, 1, {
      type: 'payload',
      node: nodeIdFromInt(1),
      payload: new Uint8Array([1, 2, 3]),
    }),
  ]);

  const [transportA, transportB, wire] = createLoggedTimedDuplex<SyncMessage<Operation>>();
  const peerA = new SyncPeer(a, {
    auth: {
      helloCapabilities: async () => [{ name: 'auth.capability', value: 'structure-only-reader' }],
    },
  });
  const peerB = new SyncPeer(b, {
    auth: {
      onHello: async () => [],
      authorizeFilter: async () => {},
      filterOutgoingOps: async (ops) => {
        if (ops.some((op) => op.kind.type === 'payload')) {
          throw new Error('operation-log projection requires read_payload');
        }
        return ops.map(() => true);
      },
    },
  });
  const detachA = peerA.attach(transportA);
  const detachB = peerB.attach(transportB);

  try {
    await expect(peerA.syncOnce(transportA, { all: {} })).rejects.toThrow(/read_payload/);

    const ack = wire.find(
      (entry) => entry.dir === 'bToA' && entry.msg.payload.case === 'helloAck',
    )?.msg;
    expect(ack?.payload.case).toBe('helloAck');
    if (ack?.payload.case === 'helloAck') {
      expect(ack.payload.value.acceptedFilters).toEqual([]);
      expect(ack.payload.value.rejectedFilters).toHaveLength(1);
    }
    expect(wire.some((entry) => entry.msg.payload.case === 'ribltCodewords')).toBe(false);
    expect(wire.some((entry) => entry.msg.payload.case === 'ribltStatus')).toBe(false);
  } finally {
    detachA();
    detachB();
  }
});

test('syncOnce rejects when local message handler throws during apply', async () => {
  const docId = 'doc-sync-apply-error';
  const root = '0'.repeat(32);

  const a = new FailingApplyBackend(docId);
  const b = new MemoryBackend(docId);
  await b.applyOps([
    makeOp(replicas.b, 1, 1, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    }),
  ]);

  const { peerA: pa, transportA: ta, detach } = createPeers(a, b);
  try {
    const timed = Promise.race([
      pa.syncOnce(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 }),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error('syncOnce timed out')), 2_000),
      ),
    ]);
    await expect(timed).rejects.toThrow(/apply failed/i);
  } finally {
    detach();
  }
});

test('sync all converges union of opRefs', async () => {
  const docId = 'doc-sync-all';
  const root = '0'.repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  await a.applyOps([
    makeOp(replicas.a, 1, 1, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    }),
    makeOp(replicas.a, 2, 2, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(2),
      orderKey: orderKeyFromPosition(0),
    }),
  ]);
  await b.applyOps([
    makeOp(replicas.b, 1, 3, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(3),
      orderKey: orderKeyFromPosition(0),
    }),
    makeOp(replicas.a, 2, 2, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(2),
      orderKey: orderKeyFromPosition(0),
    }),
  ]);

  const { peerA: pa, transportA: ta, detach } = createPeers(a, b);
  try {
    await pa.syncOnce(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });
    await tick();

    const aAll = await a.listOpRefs({ all: {} });
    const bAll = await b.listOpRefs({ all: {} });
    expect(setHex(aAll)).toEqual(setHex(bAll));
  } finally {
    detach();
  }
});

test('sync all transfers a single missing op (hole in the middle)', async () => {
  const docId = 'doc-sync-one-missing';
  const root = '0'.repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const size = 100;
  const missingCounter = Math.ceil(size / 2);
  const ops: Operation[] = [];
  for (let counter = 1; counter <= size; counter += 1) {
    ops.push(
      makeOp(replicas.s, counter, counter, {
        type: 'insert',
        parent: root,
        node: nodeIdFromInt(counter),
        orderKey: orderKeyFromPosition(counter - 1),
      }),
    );
  }

  await b.applyOps(ops);
  await a.applyOps(ops.filter((op) => op.meta.id.counter !== missingCounter));

  const { peerA: pa, transportA: ta, detach } = createPeers(a, b);
  try {
    await pa.syncOnce(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });
    await tick();

    expect(a.hasOp(replicaHex.s, missingCounter)).toBe(true);
    const aAll = await a.listOpRefs({ all: {} });
    const bAll = await b.listOpRefs({ all: {} });
    expect(setHex(aAll)).toEqual(setHex(bAll));
  } finally {
    detach();
  }
});

test('sync children(parent) only transfers those children', async () => {
  const docId = 'doc-sync-children';
  const parentAHex = 'a0'.repeat(16);
  const parentBHex = 'b0'.repeat(16);
  const parentABytes = nodeIdToBytes16(parentAHex);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  await a.applyOps([
    makeOp(replicas.a, 1, 1, {
      type: 'insert',
      parent: parentAHex,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    }),
    makeOp(replicas.a, 2, 2, {
      type: 'insert',
      parent: parentBHex,
      node: nodeIdFromInt(2),
      orderKey: orderKeyFromPosition(0),
    }),
  ]);
  await b.applyOps([
    makeOp(replicas.b, 1, 3, {
      type: 'insert',
      parent: parentAHex,
      node: nodeIdFromInt(3),
      orderKey: orderKeyFromPosition(0),
    }),
    makeOp(replicas.b, 2, 4, {
      type: 'insert',
      parent: parentBHex,
      node: nodeIdFromInt(4),
      orderKey: orderKeyFromPosition(0),
    }),
  ]);

  const { peerA: pa, transportA: ta, detach } = createPeers(a, b);
  try {
    await pa.syncOnce(
      ta,
      { children: { parent: parentABytes } },
      { maxCodewords: 10_000, codewordsPerMessage: 256 },
    );
    await tick();

    // Converges for the filtered view.
    const aChildrenA = await a.listOpRefs({ children: { parent: parentABytes } });
    const bChildrenA = await b.listOpRefs({ children: { parent: parentABytes } });
    expect(setHex(aChildrenA)).toEqual(setHex(bChildrenA));

    // Does not leak ops outside the filter.
    expect(a.hasOp(replicaHex.b, 1)).toBe(true);
    expect(a.hasOp(replicaHex.b, 2)).toBe(false);
    expect(b.hasOp(replicaHex.a, 1)).toBe(true);
    expect(b.hasOp(replicaHex.a, 2)).toBe(false);
  } finally {
    detach();
  }
});

test('sync children(parent) includes boundary-crossing moves', async () => {
  const docId = 'doc-sync-boundary-move';
  const parentAHex = 'a0'.repeat(16);
  const parentBHex = 'b0'.repeat(16);
  const parentABytes = nodeIdToBytes16(parentAHex);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const node = nodeIdFromInt(0x10);

  await a.applyOps([
    makeOp(replicas.a, 1, 1, {
      type: 'insert',
      parent: parentAHex,
      node,
      orderKey: orderKeyFromPosition(0),
    }),
    // Move the node out of the subtree. The move is still relevant to `children(parentA)`
    // because it changes the canonical child set of `parentA`.
    makeOp(replicas.a, 2, 2, {
      type: 'move',
      node,
      newParent: parentBHex,
      orderKey: orderKeyFromPosition(0),
    }),
  ]);

  const { peerA: pa, transportA: ta, detach } = createPeers(a, b);
  try {
    await pa.syncOnce(
      ta,
      { children: { parent: parentABytes } },
      { maxCodewords: 10_000, codewordsPerMessage: 256 },
    );
    await tick();

    expect(b.hasOp(replicaHex.a, 2)).toBe(true);

    const aChildrenA = await a.listOpRefs({ children: { parent: parentABytes } });
    const bChildrenA = await b.listOpRefs({ children: { parent: parentABytes } });
    expect(setHex(aChildrenA)).toEqual(setHex(bChildrenA));
  } finally {
    detach();
  }
});

test('sync children(parent) includes latest payload when node moves into parent', async () => {
  const docId = 'doc-sync-children-payload';
  const parentAHex = 'a0'.repeat(16);
  const parentBHex = 'b0'.repeat(16);
  const parentBBytes = nodeIdToBytes16(parentBHex);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  const node = nodeIdFromInt(0x10);
  const payload = new Uint8Array([1, 2, 3]);

  await a.applyOps([
    makeOp(replicas.a, 1, 1, {
      type: 'insert',
      parent: parentAHex,
      node,
      orderKey: orderKeyFromPosition(0),
    }),
    makeOp(replicas.a, 2, 2, { type: 'payload', node, payload }),
    makeOp(replicas.a, 3, 3, {
      type: 'move',
      node,
      newParent: parentBHex,
      orderKey: orderKeyFromPosition(0),
    }),
  ]);

  const { peerA: pa, transportA: ta, detach } = createPeers(a, b);
  try {
    await pa.syncOnce(
      ta,
      { children: { parent: parentBBytes } },
      { maxCodewords: 10_000, codewordsPerMessage: 256 },
    );
    await tick();

    expect(b.hasOp(replicaHex.a, 1)).toBe(false);
    expect(b.hasOp(replicaHex.a, 2)).toBe(true);
    expect(b.hasOp(replicaHex.a, 3)).toBe(true);

    const aChildrenB = await a.listOpRefs({ children: { parent: parentBBytes } });
    const bChildrenB = await b.listOpRefs({ children: { parent: parentBBytes } });
    expect(setHex(aChildrenB)).toEqual(setHex(bChildrenB));

    const ops = await b.getOpsByOpRefs(bChildrenB);
    const payloadOps = ops.filter((op) => op.kind.type === 'payload' && op.kind.payload !== null);
    expect(payloadOps.length).toBe(1);
    expect(payloadOps[0]?.kind.type).toBe('payload');
    expect(payloadOps[0]?.kind.payload).toEqual(payload);
  } finally {
    detach();
  }
});

test('subscribe keeps peers converging (push deltas)', async () => {
  const docId = 'doc-subscribe';
  const root = '0'.repeat(32);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);

  await a.applyOps([
    makeOp(replicas.a, 1, 1, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    }),
  ]);

  const { peerA: pa, peerB: pb, transportA: ta, detach } = createPeers(a, b);
  try {
    const sub = pa.subscribe(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });
    try {
      await waitUntil(() => b.hasOp(replicaHex.a, 1), {
        message: 'expected b to receive a:1 via subscription',
      });

      await b.applyOps([
        makeOp(replicas.b, 1, 2, {
          type: 'insert',
          parent: root,
          node: nodeIdFromInt(2),
          orderKey: orderKeyFromPosition(0),
        }),
      ]);
      await pb.notifyLocalUpdate();
      await waitUntil(() => a.hasOp(replicaHex.b, 1), {
        message: 'expected a to receive b:1 via subscription',
      });
    } finally {
      sub.stop();
      await sub.done;
    }
  } finally {
    detach();
  }
});

test('subscribe pushes exact all-filter deltas without rescanning full state', async () => {
  const docId = 'doc-subscribe-direct-delta-all';
  const root = '0'.repeat(32);

  const a = new MemoryBackend(docId);
  const b = new CountingListBackend(docId);

  await a.applyOps([
    makeOp(replicas.a, 1, 1, {
      type: 'insert',
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    }),
  ]);

  const {
    peerA: pa,
    peerB: pb,
    transportA: ta,
    detach,
  } = createInMemoryConnectedPeers({
    backendA: a,
    backendB: b,
    codec: treecrdtSyncV0ProtobufCodec,
    peerBOptions: {
      deriveOpRef: (op) => opRefFor(docId, bytesToHex(op.meta.id.replica), op.meta.id.counter),
    },
  });
  try {
    const sub = pa.subscribe(ta, { all: {} }, { maxCodewords: 10_000, codewordsPerMessage: 256 });
    try {
      await waitUntil(() => b.hasOp(replicaHex.a, 1), {
        message: 'expected initial subscribe catch-up',
      });
      const scanCountAfterCatchUp = b.listOpRefsCalls;

      const op = makeOp(replicas.b, 1, 2, {
        type: 'insert',
        parent: root,
        node: nodeIdFromInt(2),
        orderKey: orderKeyFromPosition(0),
      });
      await b.applyOps([op]);
      await pb.notifyLocalUpdate([op]);

      await waitUntil(() => a.hasOp(replicaHex.b, 1), {
        message: 'expected direct all-filter delta push to arrive',
      });
      expect(b.listOpRefsCalls).toBe(scanCountAfterCatchUp);
    } finally {
      sub.stop();
      await sub.done;
    }
  } finally {
    detach();
  }
});

test('subscribe refreshes replay capabilities before pushing newly authorized ops', async () => {
  const docId = 'doc-subscribe-capability-refresh';
  const root = '0'.repeat(32);
  const proofRef = new Uint8Array([0x12, 0x34, 0x56, 0x78]);
  const replayCapValue = bytesToHex(proofRef);

  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);
  const [transportA, transportB, wire] = createLoggedTimedDuplex<SyncMessage<Operation>>();
  const knownReplayCaps = new Set<string>();
  let serverHelloCaps: Array<{ name: string; value: string }> = [];
  const clientHelloCaps = [{ name: 'auth.capability', value: 'client-token' }];

  const peerA = new SyncPeer(a, {
    auth: {
      helloCapabilities: async () => serverHelloCaps,
      onHello: async () => serverHelloCaps,
      signOps: async (ops) => ops.map(() => ({ sig: new Uint8Array([1]), proofRef })),
    },
    maxOpsPerBatch: 1,
  });
  const peerB = new SyncPeer(b, {
    auth: {
      helloCapabilities: async () => clientHelloCaps,
      onHello: async (hello) => {
        for (const cap of hello.capabilities) {
          if (cap.name === 'auth.capability.replay') knownReplayCaps.add(cap.value);
        }
        return clientHelloCaps;
      },
      verifyOps: async (_ops, auth) => {
        if (!auth) throw new Error('expected auth on subscribed push');
        for (const entry of auth) {
          if (!entry?.proofRef) throw new Error('expected proofRef on subscribed push');
          if (!knownReplayCaps.has(bytesToHex(entry.proofRef))) {
            throw new Error('missing replay capability for pushed proofRef');
          }
        }
      },
    },
    maxOpsPerBatch: 1,
  });

  const detachA = peerA.attach(transportA);
  const detachB = peerB.attach(transportB);

  try {
    const sub = peerB.subscribe(transportB, { all: {} }, { immediate: false, intervalMs: 0 });
    try {
      await waitUntil(() => (peerA as any).responderSubscriptions?.size === 1, {
        message: 'expected responder subscription to be registered',
      });

      serverHelloCaps = [{ name: 'auth.capability.replay', value: replayCapValue }];
      await a.applyOps([
        makeOp(replicas.a, 1, 1, {
          type: 'insert',
          parent: root,
          node: nodeIdFromInt(11),
          orderKey: orderKeyFromPosition(0),
        }),
      ]);
      await peerA.notifyLocalUpdate();

      await waitUntil(() => b.hasOp(replicaHex.a, 1), {
        message: 'expected subscriber to accept live op after capability refresh',
      });
      expect(knownReplayCaps).toEqual(new Set([replayCapValue]));

      const serverCases = wire
        .filter((entry) => entry.dir === 'aToB')
        .map((entry) => entry.msg.payload.case);
      expect(serverCases).toContain('hello');
      expect(serverCases).toContain('opsBatch');
      expect(serverCases.indexOf('hello')).toBeLessThan(serverCases.lastIndexOf('opsBatch'));
    } finally {
      sub.stop();
      await sub.done;
    }
  } finally {
    detachA();
    detachB();
  }
});

test('subscription withdrawal sends no ops and terminates the subscriber', async () => {
  const docId = 'doc-subscribe-reader-withdrawal';
  const root = '0'.repeat(32);
  const a = new MemoryBackend(docId);
  const b = new MemoryBackend(docId);
  let readerCapabilities = [{ name: 'auth.capability', value: 'reader' }];

  const [transportA, transportB] = createTimedDuplex<SyncMessage<Operation>>();
  const peerA = new SyncPeer(a, {
    auth: {
      helloCapabilities: async () => [{ name: 'auth.capability', value: 'server' }],
      onHello: async () => [{ name: 'auth.capability', value: 'server' }],
      authorizeFilter: async () => {},
      filterOutgoingOps: async (ops, ctx) => {
        if (!ctx.capabilities.some((capability) => capability.value === 'reader')) {
          throw new Error('reader capability withdrawn');
        }
        return ops.map(() => true);
      },
    },
  });
  const peerB = new SyncPeer(b, {
    auth: {
      helloCapabilities: async () => readerCapabilities,
      onHello: async () => readerCapabilities,
    },
  });
  const detachA = peerA.attach(transportA);
  const detachB = peerB.attach(transportB);
  const sub = peerB.subscribe(transportB, { all: {} }, { immediate: false });

  try {
    await sub.ready;
    readerCapabilities = [];
    await sendHello(transportB, docId, []);

    await a.applyOps([
      makeOp(replicas.a, 1, 1, {
        type: 'insert',
        parent: root,
        node: nodeIdFromInt(1),
        orderKey: orderKeyFromPosition(0),
      }),
    ]);
    const terminal = expect(sub.done).rejects.toThrow(/reader capability withdrawn/);
    await peerA.notifyLocalUpdate();

    await terminal;
    expect(b.hasOp(replicaHex.a, 1)).toBe(false);
  } finally {
    sub.stop();
    detachA();
    detachB();
  }
});
