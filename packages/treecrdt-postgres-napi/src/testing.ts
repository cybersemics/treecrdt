import type { BenchmarkFixtureHelpers } from '@treecrdt/benchmark/testing';
import type { Operation } from '@treecrdt/interface';
import { nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';

import { createPostgresNapiAdapterFactory, type PostgresNapiAdapterFactory } from './adapter.js';
import { createTreecrdtPostgresClient } from './client.js';
import { operationToNativeWithSerializers } from './codec.js';
import {
  createPostgresNapiSyncBackendFactory,
  type PostgresNapiSyncBackendFactory,
} from './index.js';
import { loadNative, type NativeOp } from './native.js';

export { createTreecrdtPostgresClient } from './client.js';

type PostgresNapiFixtureHelpers = Pick<
  BenchmarkFixtureHelpers,
  | 'resetForTests'
  | 'resetDocForTests'
  | 'cloneDocForTests'
  | 'cloneMaterializedDocForTests'
  | 'primeDocForTests'
  | 'primeBalancedFanoutDocForTests'
>;

type PostgresNapiSyncFixtureHelpers = Pick<
  BenchmarkFixtureHelpers,
  'resetForTests' | 'resetDocForTests' | 'cloneDocForTests' | 'primeBalancedFanoutDocForTests'
>;

export type PostgresNapiTestAdapterFactory = PostgresNapiAdapterFactory &
  PostgresNapiFixtureHelpers;

export type PostgresNapiTestSyncBackendFactory = PostgresNapiSyncBackendFactory &
  PostgresNapiSyncFixtureHelpers;

type NativeTestingFactory = {
  resetForTests(): void;
  resetDocForTests(docId: string): void;
  cloneDocForTests(sourceDocId: string, targetDocId: string): void;
  cloneMaterializedDocForTests(sourceDocId: string, targetDocId: string): void;
  primeDocForTests(docId: string, ops: NativeOp[]): void;
  primeBalancedFanoutDocForTests(
    docId: string,
    size: number,
    fanout: number,
    payloadBytes: number,
    replicaLabel: string,
  ): void;
};

function ensureNonEmptyString(name: string, value: string): void {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
}

function ensurePositiveInteger(name: string, value: number): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer, got ${String(value)}`);
  }
}

function ensureNonNegativeInteger(name: string, value: number): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer, got ${String(value)}`);
  }
}

function opToNative(op: Operation) {
  return operationToNativeWithSerializers(op, nodeIdToBytes16, replicaIdToBytes);
}

function createTestingFactory(url: string): NativeTestingFactory {
  const native = loadNative() as unknown as {
    PgTestingFactory: new (url: string) => NativeTestingFactory;
  };
  return new native.PgTestingFactory(url);
}

function ensureDocPair(sourceDocId: string, targetDocId: string): void {
  ensureNonEmptyString('sourceDocId', sourceDocId);
  ensureNonEmptyString('targetDocId', targetDocId);
}

function ensureBalancedFixtureArgs(
  docId: string,
  size: number,
  fanout: number,
  payloadBytes: number,
  replicaLabel: string,
): void {
  ensureNonEmptyString('docId', docId);
  ensureNonEmptyString('replicaLabel', replicaLabel);
  ensurePositiveInteger('size', size);
  ensurePositiveInteger('fanout', fanout);
  ensureNonNegativeInteger('payloadBytes', payloadBytes);
}

function createCommonFixtureHelpers(factory: NativeTestingFactory) {
  return {
    resetForTests: async () => factory.resetForTests(),
    resetDocForTests: async (docId: string) => {
      ensureNonEmptyString('docId', docId);
      factory.resetDocForTests(docId);
    },
    cloneDocForTests: async (sourceDocId: string, targetDocId: string) => {
      ensureDocPair(sourceDocId, targetDocId);
      factory.cloneDocForTests(sourceDocId, targetDocId);
    },
    primeBalancedFanoutDocForTests: async (
      docId: string,
      size: number,
      fanout: number,
      payloadBytes: number,
      replicaLabel: string,
    ) => {
      ensureBalancedFixtureArgs(docId, size, fanout, payloadBytes, replicaLabel);
      factory.primeBalancedFanoutDocForTests(docId, size, fanout, payloadBytes, replicaLabel);
    },
  };
}

export function createPostgresNapiTestAdapterFactory(url: string): PostgresNapiTestAdapterFactory {
  ensureNonEmptyString('url', url);
  const base = createPostgresNapiAdapterFactory(url);
  const factory = createTestingFactory(url);

  return {
    ...base,
    ...createCommonFixtureHelpers(factory),
    cloneMaterializedDocForTests: async (sourceDocId: string, targetDocId: string) => {
      ensureDocPair(sourceDocId, targetDocId);
      factory.cloneMaterializedDocForTests(sourceDocId, targetDocId);
    },
    primeDocForTests: async (docId: string, ops: Operation[]) => {
      ensureNonEmptyString('docId', docId);
      factory.primeDocForTests(docId, ops.map(opToNative));
    },
  };
}

export function createPostgresNapiTestSyncBackendFactory(
  url: string,
): PostgresNapiTestSyncBackendFactory {
  ensureNonEmptyString('url', url);
  const base = createPostgresNapiSyncBackendFactory(url);
  const factory = createTestingFactory(url);

  return {
    ...base,
    ...createCommonFixtureHelpers(factory),
  };
}
