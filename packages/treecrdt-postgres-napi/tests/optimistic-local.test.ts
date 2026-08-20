import { expect, test, vi } from 'vitest';

import type { Operation } from '@treecrdt/interface';

import { commitOptimisticAuthorizedLocalWrite } from '../dist/optimistic-local.js';

const root = '0'.repeat(32);
const replica = Uint8Array.from({ length: 32 }, (_, index) => (index === 31 ? 1 : 0));

function node(value: number): string {
  return value.toString(16).padStart(32, '0');
}

function insert(counter: number, target: string): Operation {
  return {
    meta: { id: { replica, counter }, lamport: counter },
    kind: {
      type: 'insert',
      parent: root,
      node: target,
      orderKey: Uint8Array.of(counter),
    },
  };
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function proofPersistingCommit(operation: Operation) {
  const proofRows: Uint8Array[] = [];
  const commit = vi.fn((proof: { sig: Uint8Array; proofRef: Uint8Array }) => {
    proofRows.push(proof.sig);
    return { operation, value: operation };
  });
  return { commit, proofRows };
}

test('authorization holds no lock and a conflict gets a fresh op, auth, and atomic proof', async () => {
  let revision = 0;
  const firstTarget = node(1);
  const secondTarget = node(2);
  const firstAuthorizationStarted = deferred();
  const releaseFirstAuthorization = deferred();
  const emitted: string[] = [];
  const atomicallyPersisted: string[] = [];
  const firstAuthorizedCounters: number[] = [];

  const prepare = (target: string) => {
    const proposalRevision = revision;
    const operation = insert(proposalRevision + 1, target);
    return {
      operation,
      commit: (proof: { sig: Uint8Array; proofRef: Uint8Array }) => {
        if (revision !== proposalRevision) return null;
        revision += 1;
        atomicallyPersisted.push(`${target}-${proof.sig[0]}`);
        return { operation, value: { operation } };
      },
    };
  };
  const proofFor = (ops: readonly Operation[]) => [
    {
      sig: new Uint8Array(64).fill(ops[0]!.meta.id.counter),
      proofRef: new Uint8Array(16),
    },
  ];
  const firstAuth = {
    authorizeLocalOps: vi.fn(async (ops: readonly Operation[]) => {
      firstAuthorizedCounters.push(ops[0]!.meta.id.counter);
      if (firstAuthorizedCounters.length === 1) {
        firstAuthorizationStarted.resolve();
        await releaseFirstAuthorization.promise;
      }
      return proofFor(ops);
    }),
  };
  const secondAuth = {
    authorizeLocalOps: vi.fn(async (ops: readonly Operation[]) => proofFor(ops)),
  };
  const run = (target: string, authSession: typeof firstAuth | typeof secondAuth) =>
    commitOptimisticAuthorizedLocalWrite({
      authSession,
      prepare: () => prepare(target),
      onCommitted: (value) => emitted.push(value.operation.kind.node),
    });

  const first = run(firstTarget, firstAuth);
  await firstAuthorizationStarted.promise;

  // This is the deadlock regression: a second synchronous native write completes while the first
  // authorization promise is intentionally unresolved.
  await expect(run(secondTarget, secondAuth)).resolves.toMatchObject({
    kind: { node: secondTarget },
  });
  releaseFirstAuthorization.resolve();
  await expect(first).resolves.toMatchObject({ kind: { node: firstTarget } });

  expect(firstAuthorizedCounters).toEqual([1, 2]);
  expect(atomicallyPersisted).toEqual([`${secondTarget}-1`, `${firstTarget}-2`]);
  expect(emitted).toEqual([secondTarget, firstTarget]);
  expect(revision).toBe(2);
});

test('invalid proof fails before native commit', async () => {
  const target = node(3);
  const operation = insert(1, target);
  const commit = vi.fn(() => ({ operation, value: operation }));

  await expect(
    commitOptimisticAuthorizedLocalWrite({
      authSession: {
        authorizeLocalOps: async () => [{ sig: Uint8Array.of(1), proofRef: Uint8Array.of(1) }],
      },
      prepare: () => ({
        operation,
        commit,
      }),
      onCommitted: () => {},
    }),
  ).rejects.toThrow(/invalid operation proof/);
  expect(commit).not.toHaveBeenCalled();
});

test('proof buffers are snapshotted before native commit', async () => {
  const operation = insert(1, node(31));
  const sig = new Uint8Array(64).fill(7);
  const proofRef = new Uint8Array(16).fill(8);
  const commit = vi.fn((proof: { sig: Uint8Array; proofRef: Uint8Array }) => {
    expect(proof.sig).not.toBe(sig);
    expect(proof.proofRef).not.toBe(proofRef);
    proof.sig[0] = 9;
    proof.proofRef[0] = 10;
    return { operation, value: operation };
  });

  await expect(
    commitOptimisticAuthorizedLocalWrite({
      authSession: {
        authorizeLocalOps: async () => [{ sig, proofRef }],
      },
      prepare: () => ({ operation, commit }),
      onCommitted: () => {},
    }),
  ).resolves.toEqual(operation);

  expect(sig[0]).toBe(7);
  expect(proofRef[0]).toBe(8);
});

test('authorization metadata mutation cannot reach native commit or proof storage', async () => {
  const target = node(4);
  const operation = insert(1, target);
  const { commit, proofRows } = proofPersistingCommit(operation);

  await expect(
    commitOptimisticAuthorizedLocalWrite({
      authSession: {
        authorizeLocalOps: async (ops) => {
          ops[0]!.meta.id.counter += 1;
          return [{ sig: new Uint8Array(64), proofRef: new Uint8Array(16) }];
        },
      },
      prepare: () => ({ operation, commit }),
      onCommitted: () => {},
    }),
  ).rejects.toThrow(/mutated the proposed operation/);

  expect(operation.meta.id.counter).toBe(1);
  expect(commit).not.toHaveBeenCalled();
  expect(proofRows).toEqual([]);
});
